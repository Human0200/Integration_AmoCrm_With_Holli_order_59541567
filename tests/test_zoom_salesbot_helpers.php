<?php
declare(strict_types=1);
require_once __DIR__ . '/bootstrap.php';
define('ZOOM_SALESBOT_TEST_MODE', true);
require_once tests_project_root() . '/zoom_salesbot_reminders.php';

$config = ['enabled' => true, 'pipeline_id' => 8117846, 'bot_id' => 25585, 'day_bot_id' => 25585, 'hour_bot_id' => 25589, 'start_field_id' => 1639961, 'end_field_id' => 1639963, 'state_dir' => sys_get_temp_dir() . '/zoom_salesbot_' . getmypid()];
$lead = ['id' => 10, 'pipeline_id' => 8117846, 'custom_fields_values' => [
    ['field_id' => 1639961, 'values' => [['value' => '1800000000']]],
    ['field_id' => 1639963, 'values' => [['value' => '1800003600']]],
]];
tests_assert_same(['lead_id' => 10, 'start_at' => 1800000000, 'end_at' => 1800003600, 'updated_at' => 0], array_merge(zoom_salesbot_schedule_from_lead($lead, $config), ['updated_at' => 0]), 'Both Zoom fields should be required.');
tests_assert_same(null, zoom_salesbot_schedule_from_lead(array_merge($lead, ['pipeline_id' => 123]), $config), 'Other pipelines must be ignored.');
tests_assert_same(null, zoom_salesbot_schedule_from_lead(['id' => 10, 'pipeline_id' => 8117846, 'custom_fields_values' => $lead['custom_fields_values'][0] ? [$lead['custom_fields_values'][0]] : []], $config), 'Missing end field must be ignored.');
tests_assert_same(['lead_id' => 10, 'start_at' => 1800000000], zoom_salesbot_start_event_from_lead($lead, $config), 'Start date should create a Salesbot event.');
tests_assert_same(['lead_id' => 10, 'start_at' => 1800000000], zoom_salesbot_start_event_from_lead([
    'id' => 10,
    'pipeline_id' => 8117846,
    'custom_fields_values' => [$lead['custom_fields_values'][0]],
], $config), 'Salesbot must not depend on the meeting end field.');
tests_assert_same(null, zoom_salesbot_start_event_from_lead(array_merge($lead, ['pipeline_id' => 123]), $config), 'Other pipelines must be ignored by Salesbot launcher.');
tests_assert_same(['kind' => 'day', 'offset' => 86400, 'bot_id' => 25585], zoom_salesbot_agent_definition(['start_at' => 1800000000, 'now_at' => 1799900000], $config), 'More than 23 hours should use one day agent.');
tests_assert_same(['kind' => 'hour', 'offset' => 3600, 'bot_id' => 25589], zoom_salesbot_agent_definition(['start_at' => 1800000000, 'now_at' => 1799917200], $config), '23 hours or less should use one hour agent.');
echo "OK\n";
