-- Copyright 2024 SmartWings / Manus AI
-- Licensed under the Apache License, Version 2.0
--
-- SmartWings TDBU (Top-Down Bottom-Up) Matter over Thread Edge Driver  V1.6
-- Changelog V1.6:
--   [FIX-13] SmartWings Levitate with DUCTECH (VendorID 0x141F / PID 0x1002) only
--            showed one rail (bottom) instead of two. Root cause: fingerprints.yml
--            incorrectly mapped this TDBU device to window-covering-battery (single
--            component profile). Fixed by mapping PID 0x1002 to
--            window-covering-tdbu-battery so the device initialises with both Top
--            and Bottom components from the first pairing, without relying on the
--            async runtime profile-switch in do_configure.
--
-- Changelog V1.5:
--   [FIX-11] Preset position gear icon (setPresetPosition) not shown in App:
--            Added windowShadePreset.supportedCommands({"presetPosition","setPresetPosition"})
--            event emission during device_init for both Top and Bottom components.
--            This is required by SmartThings App to render the gear (edit) button
--            alongside the preset execute button. Aligned with official SmartThings
--            matter-window-covering driver implementation.
--   [FIX-12] Reverted FIX-9 (battery endpoint routing): TDBU device shares a single
--            PowerSource endpoint between both rails. Reverted battery_percent_handler
--            and battery_charge_level_handler back to device:emit_event() broadcast,
--            consistent with original V1.1 behaviour.
--
-- Changelog V1.4:
--   [FIX-8]  Reverted FIX-7: removed presetTop/presetBottom preferences.
--            Preset position uses native setPresetPosition gear-icon dialog.
--   [FIX-9]  (reverted in V1.5) Battery endpoint routing.
--   [FIX-10] operational_status_handler nil-guard for reverse field.
--
-- Changelog V1.2:
--   [FIX-4] Component label "main" -> "Top" in all TDBU profiles.
--   [FIX-5] Bottom component Preset initialisation deferred to do_configure.
--   [FIX-6] Bottom component battery capability added to TDBU battery profiles.
--
-- Changelog V1.1:
--   [FIX-1] Bottom component endpoint routing via endpoint_to_component reverse lookup.
--   [FIX-2] Thread End Device wake-up retry loop (send_with_retry).
--   [FIX-3] Runtime TDBU detection in do_configure; removed matterGeneric fallback.
--
-- Device Endpoint Layout (TDBU):
--   Endpoint 0  : Root Node (descriptor, basic information, OTA, PowerSource)
--   Endpoint 1  : Top Motor   -> mapped to SmartThings component "main"   (label: Top)
--   Endpoint 2  : Bottom Motor -> mapped to SmartThings component "bottom" (label: Bottom)
--
-- TDBU Lift Semantics (CSA Matter Spec §5.3):
--   CurrentPositionLiftPercent100ths = 0     -> Fully Open   (motor at top)
--   CurrentPositionLiftPercent100ths = 10000 -> Fully Closed (motor at bottom)
--   SmartThings shadeLevel = 0   -> Closed
--   SmartThings shadeLevel = 100 -> Open
--   Conversion: shadeLevel     = 100 - math.floor(percent100ths / 100)
--   Command:    percent100ths  = (100 - shadeLevel) * 100

local capabilities  = require "st.capabilities"
local im            = require "st.matter.interaction_model"
local log           = require "log"
local clusters      = require "st.matter.clusters"
local MatterDriver  = require "st.matter.driver"

-- ============================================================
-- Constants
-- ============================================================

local FIELD_TOP_LIFT   = "__tdbu_top_lift"
local FIELD_BOT_LIFT   = "__tdbu_bot_lift"
local FIELD_REVERSE    = "__reverse_polarity"
local FIELD_PRESET_TOP = "__preset_top"
local FIELD_PRESET_BOT = "__preset_bot"
local FIELD_IS_TDBU    = "__is_tdbu"
local DEFAULT_PRESET   = 50

local COMPONENT_TOP    = "main"
local COMPONENT_BOTTOM = "bottom"

-- [FIX-2] End Device wake-up: max retries and delay between retries (seconds)
local WAKE_MAX_RETRIES = 3
local WAKE_RETRY_DELAY = 2

local battery_support = {
  NO_BATTERY         = "NO_BATTERY",
  BATTERY_LEVEL      = "BATTERY_LEVEL",
  BATTERY_PERCENTAGE = "BATTERY_PERCENTAGE",
}

-- ============================================================
-- Endpoint Discovery & Component Mapping
-- ============================================================

--- Find all endpoints that support the WindowCovering cluster.
--- Returns a sorted list of endpoint IDs (excluding endpoint 0).
local function get_window_covering_endpoints(device)
  local eps = device:get_endpoints(clusters.WindowCovering.ID)
  local result = {}
  for _, v in ipairs(eps) do
    if v ~= 0 then
      table.insert(result, v)
    end
  end
  table.sort(result)
  return result
end

--- Map a SmartThings component name to a Matter endpoint ID.
local function component_to_endpoint(device, component_name)
  local eps = get_window_covering_endpoints(device)
  if #eps == 0 then
    return device.MATTER_DEFAULT_ENDPOINT
  end
  local is_tdbu = device:get_field(FIELD_IS_TDBU)
  if component_name == COMPONENT_BOTTOM and is_tdbu and #eps >= 2 then
    return eps[2]
  end
  return eps[1]
end

--- Map a Matter endpoint ID back to a SmartThings component name.
local function endpoint_to_component(device, endpoint_id)
  local eps = get_window_covering_endpoints(device)
  local is_tdbu = device:get_field(FIELD_IS_TDBU)
  if is_tdbu and #eps >= 2 and endpoint_id == eps[2] then
    return COMPONENT_BOTTOM
  end
  return COMPONENT_TOP
end

-- ============================================================
-- Profile Selection
-- ============================================================

local function match_profile(device, bat_support, is_tdbu)
  local base
  if is_tdbu then
    base = "window-covering-tdbu"
  else
    if bat_support == battery_support.BATTERY_PERCENTAGE then
      base = "window-covering-battery"
    elseif bat_support == battery_support.BATTERY_LEVEL then
      base = "window-covering-batteryLevel"
    else
      base = "window-covering"
    end
    log.info(string.format("[TDBU] Switching profile -> %s (tdbu=false bat=%s)", base, bat_support))
    device:try_update_metadata({ profile = base })
    return
  end

  local profile_name
  if bat_support == battery_support.BATTERY_PERCENTAGE then
    profile_name = base .. "-battery"
  elseif bat_support == battery_support.BATTERY_LEVEL then
    profile_name = base .. "-batteryLevel"
  else
    profile_name = base
  end
  log.info(string.format("[TDBU] Switching profile -> %s (tdbu=%s bat=%s)",
    profile_name, tostring(is_tdbu), bat_support))
  device:try_update_metadata({ profile = profile_name })
end

-- ============================================================
-- [FIX-2] Thread End Device Wake-Up Helper
-- ============================================================

local function send_with_retry(device, request)
  local ok, err = pcall(function() device:send(request) end)
  if ok then
    log.debug("[TDBU] send_with_retry: sent on first attempt")
    return
  end
  log.warn(string.format("[TDBU] send_with_retry: attempt 1 failed (%s), scheduling retries", tostring(err)))
  for attempt = 2, WAKE_MAX_RETRIES do
    device.thread:call_with_delay(WAKE_RETRY_DELAY * (attempt - 1), function()
      local ok2, err2 = pcall(function() device:send(request) end)
      if ok2 then
        log.debug(string.format("[TDBU] send_with_retry: sent on attempt %d", attempt))
      else
        log.warn(string.format("[TDBU] send_with_retry: attempt %d failed (%s)", attempt, tostring(err2)))
      end
    end)
  end
end

-- ============================================================
-- Preset Helpers
-- ============================================================

--- Read the persisted preset value for a component.
--- Falls back to DEFAULT_PRESET (50) if not yet set.
local function get_preset_value(device, field)
  local stored = device:get_field(field)
  if stored ~= nil then return stored end
  return DEFAULT_PRESET
end

--- [FIX-11] Initialise preset position for a single component/endpoint pair.
--- Emits BOTH supportedCommands (to show the gear edit icon in App) AND
--- position (to show the numeric value) if not yet initialised.
--- This matches the official SmartThings matter-window-covering driver behaviour.
local function init_preset_for_component(device, component, field)
  if not device:supports_capability_by_id(capabilities.windowShadePreset.ID, component) then
    return
  end
  local current = device:get_latest_state(
    component,
    capabilities.windowShadePreset.ID,
    capabilities.windowShadePreset.position.NAME
  )
  if current == nil then
    local preset = get_preset_value(device, field)
    local ep = component_to_endpoint(device, component)
    -- Must emit supportedCommands first so App renders the gear (edit) button
    device:emit_event_for_endpoint(ep,
      capabilities.windowShadePreset.supportedCommands(
        { "presetPosition", "setPresetPosition" },
        { visibility = { displayed = false } }
      )
    )
    device:emit_event_for_endpoint(ep,
      capabilities.windowShadePreset.position(preset, { visibility = { displayed = false } })
    )
    device:set_field(field, preset, { persist = true })
    log.debug(string.format("[TDBU] Preset init -> component=%s ep=%d preset=%d", component, ep, preset))
  end
end

--- Persist and emit a new preset value for a component.
--- Called by handle_set_preset (gear-icon dialog in App).
local function apply_preset(device, component, field, new_preset)
  new_preset = math.max(0, math.min(100, math.floor(tonumber(new_preset) or DEFAULT_PRESET)))
  device:set_field(field, new_preset, { persist = true })
  local ep = component_to_endpoint(device, component)
  device:emit_event_for_endpoint(ep, capabilities.windowShadePreset.position(new_preset))
  log.info(string.format("[TDBU] Preset saved -> component=%s ep=%d position=%d", component, ep, new_preset))
end

-- ============================================================
-- Lifecycle Handlers
-- ============================================================

local function device_added(driver, device)
  device:emit_event(
    capabilities.windowShade.supportedWindowShadeCommands(
      { "open", "close", "pause" },
      { visibility = { displayed = false } }
    )
  )
  device:set_field(FIELD_REVERSE, false, { persist = true })
end

local function device_init(driver, device)
  device:set_component_to_endpoint_fn(component_to_endpoint)
  device:set_endpoint_to_component_fn(endpoint_to_component)

  -- Initialise TOP preset here.
  -- BOTTOM preset is deferred to do_configure because FIELD_IS_TDBU is not yet
  -- set at device_init time, so component_to_endpoint would return the wrong ep.
  init_preset_for_component(device, COMPONENT_TOP, FIELD_PRESET_TOP)

  device:subscribe()
end

local function do_configure(driver, device)
  local eps = get_window_covering_endpoints(device)
  local is_tdbu = (#eps >= 2)
  device:set_field(FIELD_IS_TDBU, is_tdbu, { persist = true })
  log.info(string.format("[TDBU] do_configure: found %d WindowCovering endpoints -> is_tdbu=%s",
    #eps, tostring(is_tdbu)))

  if is_tdbu then
    device:emit_event_for_endpoint(
      eps[2],
      capabilities.windowShade.supportedWindowShadeCommands(
        { "open", "close", "pause" },
        { visibility = { displayed = false } }
      )
    )
    local sub_req = im.InteractionRequest(im.InteractionRequest.RequestType.SUBSCRIBE, {})
    sub_req:merge(
      clusters.WindowCovering.attributes.CurrentPositionLiftPercent100ths:subscribe(device, eps[2])
    )
    sub_req:merge(
      clusters.WindowCovering.attributes.OperationalStatus:subscribe(device, eps[2])
    )
    device:send(sub_req)
    log.info(string.format("[TDBU] Subscribed to ep%d (bottom motor) attributes", eps[2]))

    -- [FIX-5] Initialise bottom preset after FIELD_IS_TDBU is set
    init_preset_for_component(device, COMPONENT_BOTTOM, FIELD_PRESET_BOT)
  end

  local battery_eps = device:get_endpoints(
    clusters.PowerSource.ID,
    { feature_bitmap = clusters.PowerSource.types.PowerSourceFeature.BATTERY }
  )
  if #battery_eps > 0 then
    local read_req = im.InteractionRequest(im.InteractionRequest.RequestType.READ, {})
    read_req:merge(clusters.PowerSource.attributes.AttributeList:read())
    device:send(read_req)
    device:set_field("__pending_is_tdbu", is_tdbu, { persist = false })
  else
    match_profile(device, battery_support.NO_BATTERY, is_tdbu)
  end
end

local function info_changed(driver, device, event, args)
  if device.profile.id ~= args.old_st_store.profile.id then
    device:subscribe()
    return
  end

  local old_prefs = args.old_st_store.preferences or {}
  local new_prefs = device.preferences or {}

  -- Reverse polarity toggle
  if old_prefs.reverse ~= new_prefs.reverse then
    device:set_field(FIELD_REVERSE, new_prefs.reverse == true, { persist = true })
    log.info(string.format("[TDBU] Preference changed: reverse = %s", tostring(new_prefs.reverse)))
  end
end

local function device_removed(driver, device)
  log.info(string.format("[TDBU] Device removed: %s", device.label))
end

-- ============================================================
-- Capability Command Handlers (Downlink: App -> Device)
-- ============================================================

local function handle_open(driver, device, cmd)
  local ep = device:component_to_endpoint(cmd.component)
  local reverse = device:get_field(FIELD_REVERSE) or false
  local req = reverse
    and clusters.WindowCovering.server.commands.DownOrClose(device, ep)
    or  clusters.WindowCovering.server.commands.UpOrOpen(device, ep)
  send_with_retry(device, req)
  log.debug(string.format("[TDBU] Open -> component=%s ep=%d reverse=%s", cmd.component, ep, tostring(reverse)))
end

local function handle_close(driver, device, cmd)
  local ep = device:component_to_endpoint(cmd.component)
  local reverse = device:get_field(FIELD_REVERSE) or false
  local req = reverse
    and clusters.WindowCovering.server.commands.UpOrOpen(device, ep)
    or  clusters.WindowCovering.server.commands.DownOrClose(device, ep)
  send_with_retry(device, req)
  log.debug(string.format("[TDBU] Close -> component=%s ep=%d", cmd.component, ep))
end

local function handle_pause(driver, device, cmd)
  local ep = device:component_to_endpoint(cmd.component)
  send_with_retry(device, clusters.WindowCovering.server.commands.StopMotion(device, ep))
  log.debug(string.format("[TDBU] Pause -> component=%s ep=%d", cmd.component, ep))
end

local function handle_shade_level(driver, device, cmd)
  local ep = device:component_to_endpoint(cmd.component)
  local shade_level = cmd.args.shadeLevel
  local percent100ths = (100 - shade_level) * 100
  send_with_retry(device,
    clusters.WindowCovering.server.commands.GoToLiftPercentage(device, ep, percent100ths)
  )
  log.debug(string.format("[TDBU] SetLevel -> component=%s ep=%d level=%d percent100ths=%d",
    cmd.component, ep, shade_level, percent100ths))
end

--- presetPosition command: move to the stored preset value.
--- Reads from the component's own latest state (set by setPresetPosition),
--- falling back to the persisted field, then to DEFAULT_PRESET.
local function handle_preset(driver, device, cmd)
  local ep = device:component_to_endpoint(cmd.component)
  local field = (cmd.component == COMPONENT_BOTTOM) and FIELD_PRESET_BOT or FIELD_PRESET_TOP
  -- Read from latest App state first (set by gear-icon dialog), then persisted field
  local preset = device:get_latest_state(
    cmd.component,
    capabilities.windowShadePreset.ID,
    capabilities.windowShadePreset.position.NAME
  ) or get_preset_value(device, field)
  local percent100ths = (100 - preset) * 100
  send_with_retry(device,
    clusters.WindowCovering.server.commands.GoToLiftPercentage(device, ep, percent100ths)
  )
  log.debug(string.format("[TDBU] Preset -> component=%s ep=%d preset=%d", cmd.component, ep, preset))
end

--- setPresetPosition command: called when user taps the gear icon and saves a new value.
--- Persists the value and emits a position state event so the App reflects the change.
local function handle_set_preset(driver, device, cmd)
  local field = (cmd.component == COMPONENT_BOTTOM) and FIELD_PRESET_BOT or FIELD_PRESET_TOP
  apply_preset(device, cmd.component, field, cmd.args.position)
end

local function handle_refresh(driver, device, cmd)
  local eps = get_window_covering_endpoints(device)
  local read_req = im.InteractionRequest(im.InteractionRequest.RequestType.READ, {})
  for _, ep in ipairs(eps) do
    read_req:merge(
      clusters.WindowCovering.attributes.CurrentPositionLiftPercent100ths:read(device, ep)
    )
    read_req:merge(
      clusters.WindowCovering.attributes.OperationalStatus:read(device, ep)
    )
  end
  local bat_eps = device:get_endpoints(clusters.PowerSource.ID,
    { feature_bitmap = clusters.PowerSource.types.PowerSourceFeature.BATTERY })
  for _, ep in ipairs(bat_eps) do
    read_req:merge(clusters.PowerSource.attributes.BatPercentRemaining:read(device, ep))
    read_req:merge(clusters.PowerSource.attributes.BatChargeLevel:read(device, ep))
  end
  send_with_retry(device, read_req)
  log.debug("[TDBU] Refresh requested")
end

-- ============================================================
-- Matter Attribute Handlers (Uplink: Device -> App)
-- ============================================================

local function current_lift_pos_handler(driver, device, ib, response)
  if ib.data.value == nil then return end

  local percent100ths = ib.data.value
  local shade_level = 100 - math.floor(percent100ths / 100)
  local reverse = device:get_field(FIELD_REVERSE) or false
  local ep = ib.endpoint_id
  local component = endpoint_to_component(device, ep)

  device:emit_event_for_endpoint(ep, capabilities.windowShadeLevel.shadeLevel(shade_level))

  local field = (component == COMPONENT_BOTTOM) and FIELD_BOT_LIFT or FIELD_TOP_LIFT
  device:set_field(field, shade_level)

  local windowShade = capabilities.windowShade.windowShade
  local state
  if shade_level == 100 then
    state = reverse and windowShade.closed() or windowShade.open()
  elseif shade_level == 0 then
    state = reverse and windowShade.open() or windowShade.closed()
  else
    state = windowShade.partially_open()
  end
  device:emit_event_for_endpoint(ep, state)

  log.debug(string.format("[TDBU] LiftPos -> ep=%d component=%s percent100ths=%d level=%d",
    ep, component, percent100ths, shade_level))
end

-- [FIX-10] Guard against nil reverse field on first boot
local function operational_status_handler(driver, device, ib, response)
  local windowShade = capabilities.windowShade.windowShade
  local reverse = device:get_field(FIELD_REVERSE) or false
  local global_status = ib.data.value & 0x03
  local ep = ib.endpoint_id

  if global_status == 1 then
    device:emit_event_for_endpoint(ep, reverse and windowShade.closing() or windowShade.opening())
  elseif global_status == 2 then
    device:emit_event_for_endpoint(ep, reverse and windowShade.opening() or windowShade.closing())
  elseif global_status ~= 0 then
    device:emit_event_for_endpoint(ep, windowShade.unknown())
  end
end

-- [FIX-12] TDBU shares a single PowerSource endpoint; use broadcast emit_event
local function battery_percent_handler(driver, device, ib, response)
  if ib.data.value then
    local pct = math.floor(ib.data.value / 2.0 + 0.5)
    device:emit_event(capabilities.battery.battery(pct))
    log.debug(string.format("[TDBU] Battery %d%%", pct))
  end
end

-- [FIX-12] TDBU shares a single PowerSource endpoint; use broadcast emit_event
local function battery_charge_level_handler(driver, device, ib, response)
  local level_enum = clusters.PowerSource.types.BatChargeLevelEnum
  if ib.data.value == level_enum.OK then
    device:emit_event(capabilities.batteryLevel.battery.normal())
  elseif ib.data.value == level_enum.WARNING then
    device:emit_event(capabilities.batteryLevel.battery.warning())
  elseif ib.data.value == level_enum.CRITICAL then
    device:emit_event(capabilities.batteryLevel.battery.critical())
  end
end

local function power_source_attr_list_handler(driver, device, ib, response)
  local is_tdbu = device:get_field("__pending_is_tdbu")
  if is_tdbu == nil then
    is_tdbu = device:get_field(FIELD_IS_TDBU)
  end
  for _, attr in ipairs(ib.data.elements) do
    if attr.value == 0x0C then
      match_profile(device, battery_support.BATTERY_PERCENTAGE, is_tdbu)
      return
    elseif attr.value == 0x0E then
      match_profile(device, battery_support.BATTERY_LEVEL, is_tdbu)
      return
    end
  end
  match_profile(device, battery_support.NO_BATTERY, is_tdbu)
end

-- ============================================================
-- Driver Template
-- ============================================================

local matter_driver_template = {
  lifecycle_handlers = {
    init        = device_init,
    added       = device_added,
    removed     = device_removed,
    infoChanged = info_changed,
    doConfigure = do_configure,
  },

  matter_handlers = {
    attr = {
      [clusters.WindowCovering.ID] = {
        [clusters.WindowCovering.attributes.CurrentPositionLiftPercent100ths.ID] = current_lift_pos_handler,
        [clusters.WindowCovering.attributes.OperationalStatus.ID]                = operational_status_handler,
      },
      [clusters.PowerSource.ID] = {
        [clusters.PowerSource.attributes.AttributeList.ID]       = power_source_attr_list_handler,
        [clusters.PowerSource.attributes.BatPercentRemaining.ID] = battery_percent_handler,
        [clusters.PowerSource.attributes.BatChargeLevel.ID]      = battery_charge_level_handler,
      },
    },
  },

  subscribed_attributes = {
    [capabilities.windowShade.ID] = {
      clusters.WindowCovering.attributes.OperationalStatus,
    },
    [capabilities.windowShadeLevel.ID] = {
      clusters.WindowCovering.attributes.CurrentPositionLiftPercent100ths,
    },
    [capabilities.battery.ID] = {
      clusters.PowerSource.attributes.BatPercentRemaining,
    },
    [capabilities.batteryLevel.ID] = {
      clusters.PowerSource.attributes.BatChargeLevel,
    },
  },

  capability_handlers = {
    [capabilities.windowShade.ID] = {
      [capabilities.windowShade.commands.open.NAME]  = handle_open,
      [capabilities.windowShade.commands.close.NAME] = handle_close,
      [capabilities.windowShade.commands.pause.NAME] = handle_pause,
    },
    [capabilities.windowShadeLevel.ID] = {
      [capabilities.windowShadeLevel.commands.setShadeLevel.NAME] = handle_shade_level,
    },
    [capabilities.windowShadePreset.ID] = {
      [capabilities.windowShadePreset.commands.presetPosition.NAME]    = handle_preset,
      [capabilities.windowShadePreset.commands.setPresetPosition.NAME] = handle_set_preset,
    },
    [capabilities.refresh.ID] = {
      [capabilities.refresh.commands.refresh.NAME] = handle_refresh,
    },
  },

  supported_capabilities = {
    capabilities.windowShade,
    capabilities.windowShadeLevel,
    capabilities.windowShadePreset,
    capabilities.battery,
    capabilities.batteryLevel,
    capabilities.refresh,
  },

  shared_device_thread_enabled = true,
}

-- ============================================================
-- Driver Entry Point
-- ============================================================

local matter_driver = MatterDriver("matter-tdbu-window-covering", matter_driver_template)
log.info("[TDBU] SmartWings TDBU Matter Driver V1.5 starting...")
matter_driver:run()
