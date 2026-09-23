<?php

declare(strict_types=1);

require_once __DIR__ . '/bootstrap.php';

require_once tests_project_root() . '/zoom/zoom_integration.php';

$config = [
    'start_field_id' => 2001,
    'end_field_id' => 2002,
    'storage_dir' => sys_get_temp_dir() . '/codex_zoom_tests_' . getmypid(),
];
@mkdir($config['storage_dir'], 0777, true);

$lead = [
    'custom_fields_values' => [
        ['field_id' => 2001, 'values' => [['value' => '1788516000']]],
        ['field_id' => 2002, 'values' => [['value' => '1788519600']]],
    ],
];

tests_assert_same(
    ['start' => 1788516000, 'end' => 1788519600],
    zoom_lead_interval($lead, $config),
    'Zoom interval should read amoCRM timestamps.'
);

tests_assert_same(
    'https://zoom.us/rec/share/demo',
    zoom_recording_url(['share_url' => 'https://zoom.us/rec/share/demo']),
    'Zoom share_url should be preferred.'
);

tests_assert_same(
    'https://zoom.us/rec/play/demo',
    zoom_recording_url(['recording_files' => [['play_url' => 'https://zoom.us/rec/play/demo']]]),
    'Zoom play_url should be used when share_url is absent.'
);

$secret = 'test-secret';
$rawBody = '{"event":"recording.completed","payload":{"object":{"id":"123"}}}';
$payload = json_decode($rawBody, true);
$timestamp = '1788516000';
$signature = 'v0=' . hash_hmac('sha256', 'v0:' . $timestamp . ':' . $rawBody, $secret);
$payload['_raw_body'] = $rawBody;
tests_assert_true(
    zoom_verify_webhook_signature($payload, [
        'x-zm-request-timestamp' => $timestamp,
        'x-zm-signature' => $signature,
    ], $secret),
    'Valid Zoom signature should be accepted.'
);
tests_assert_true(
    !zoom_verify_webhook_signature($payload, [
        'x-zm-request-timestamp' => $timestamp,
        'x-zm-signature' => 'v0=invalid',
    ], $secret),
    'Invalid Zoom signature should be rejected.'
);

zoom_save_mapping(100, [
    'amo_deal_id' => 100,
    'zoom_meeting_id' => 'old',
    'start_at' => 1788516000,
    'end_at' => 1788519600,
    'zoom_status' => 'scheduled',
], $config);
tests_assert_true(
    zoom_has_conflict(101, ['start' => 1788517800, 'end' => 1788521400], $config),
    'Overlapping known meetings should be detected.'
);
tests_assert_true(
    !zoom_has_conflict(101, ['start' => 1788523200, 'end' => 1788526800], $config),
    'Non-overlapping meetings should not conflict.'
);

echo "OK\n";
