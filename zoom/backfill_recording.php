<?php

declare(strict_types=1);

if (PHP_SAPI !== 'cli') {
    http_response_code(404);
    exit;
}

require_once __DIR__ . '/zoom_integration.php';
require_once dirname(__DIR__) . '/amo_func.php';

$leadId = isset($argv[1]) ? (int) $argv[1] : 0;
if ($leadId <= 0) {
    fwrite(STDERR, "Usage: php zoom/backfill_recording.php <amo_lead_id>\n");
    exit(1);
}

try {
    echo json_encode(
        zoom_sync_recording_for_lead($leadId, zoom_config()),
        JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT
    ) . PHP_EOL;
} catch (Throwable $exception) {
    fwrite(STDERR, $exception->getMessage() . PHP_EOL);
    exit(1);
}
