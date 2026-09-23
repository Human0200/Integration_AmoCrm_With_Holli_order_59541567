<?php

declare(strict_types=1);

/**
 * Планировщик напоминаний Salesbot для Zoom-встреч.
 *
 * Webhook amoCRM вызывает zoom_salesbot_capture_lead() и сохраняет актуальную
 * дату сделки. Cron запускает этот файл с ключом --run и отправляет каждое
 * напоминание ровно один раз.
 */

if (!defined('ZOOM_SALESBOT_TEST_MODE')) {
    require_once __DIR__ . '/config.php';
    require_once __DIR__ . '/logger.php';
    require_once __DIR__ . '/amo_func.php';
}

const ZOOM_SALESBOT_DEFAULT_PIPELINE_ID = 8117846;
const ZOOM_SALESBOT_DEFAULT_ID = 25585;
const ZOOM_SALESBOT_STATE_DIR = __DIR__ . '/data/zoom_salesbot';

function zoom_salesbot_config(): array
{
    return [
        'enabled' => getenv('ZOOM_SALESBOT_ENABLED') !== '0',
        'pipeline_id' => (int) (getenv('ZOOM_SALESBOT_PIPELINE_ID') ?: ZOOM_SALESBOT_DEFAULT_PIPELINE_ID),
        'bot_id' => (int) (getenv('ZOOM_SALESBOT_ID') ?: ZOOM_SALESBOT_DEFAULT_ID),
        'day_bot_id' => (int) (getenv('ZOOM_SALESBOT_DAY_ID') ?: getenv('ZOOM_SALESBOT_ID') ?: ZOOM_SALESBOT_DEFAULT_ID),
        'hour_bot_id' => (int) (getenv('ZOOM_SALESBOT_HOUR_ID') ?: 0),
        'start_field_id' => (int) (getenv('ZOOM_START_FIELD_ID') ?: 1639961),
        'end_field_id' => (int) (getenv('ZOOM_END_FIELD_ID') ?: 1639963),
        'state_dir' => getenv('ZOOM_SALESBOT_STATE_DIR') ?: ZOOM_SALESBOT_STATE_DIR,
    ];
}

function zoom_salesbot_field_timestamp(array $lead, int $fieldId): ?int
{
    foreach ($lead['custom_fields_values'] ?? [] as $field) {
        if ((int) ($field['field_id'] ?? 0) !== $fieldId) {
            continue;
        }
        $value = $field['values'][0]['value'] ?? null;
        if (is_numeric($value) && (int) $value > 0) {
            return (int) $value;
        }
        if (is_string($value) && ($timestamp = strtotime($value)) !== false) {
            return $timestamp;
        }
    }
    return null;
}

function zoom_salesbot_schedule_from_lead(array $lead, array $config): ?array
{
    if ((int) ($lead['pipeline_id'] ?? 0) !== $config['pipeline_id']) {
        return null;
    }
    $start = zoom_salesbot_field_timestamp($lead, $config['start_field_id']);
    $end = zoom_salesbot_field_timestamp($lead, $config['end_field_id']);
    if ($start === null || $end === null || $end <= $start) {
        return null;
    }
    return [
        'lead_id' => (int) ($lead['id'] ?? 0),
        'start_at' => $start,
        'end_at' => $end,
        'updated_at' => time(),
    ];
}

function zoom_salesbot_state_path(int $leadId, array $config): string
{
    return rtrim($config['state_dir'], '/') . '/lead_' . $leadId . '.json';
}

function zoom_salesbot_save_schedule(array $schedule, array $config): void
{
    if (!is_dir($config['state_dir'])) {
        @mkdir($config['state_dir'], 0750, true);
    }
    file_put_contents(
        zoom_salesbot_state_path((int) $schedule['lead_id'], $config),
        json_encode($schedule, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        LOCK_EX
    );
}

function zoom_salesbot_start_event_from_lead(array $lead, array $config): ?array
{
    if ((int) ($lead['pipeline_id'] ?? 0) !== $config['pipeline_id']) {
        return null;
    }

    $leadId = (int) ($lead['id'] ?? 0);
    $startAt = zoom_salesbot_field_timestamp($lead, $config['start_field_id']);
    if ($leadId <= 0 || $startAt === null) {
        return null;
    }

    return ['lead_id' => $leadId, 'start_at' => $startAt];
}

function zoom_salesbot_agent_definition(array $event, array $config): array
{
    $now = (int) ($event['now_at'] ?? time());
    $secondsUntilMeeting = (int) $event['start_at'] - $now;
    if ($secondsUntilMeeting > 23 * 3600) {
        return [
            'kind' => 'day',
            'offset' => 86400,
            'bot_id' => (int) ($config['day_bot_id'] ?? $config['bot_id']),
        ];
    }

    return [
        'kind' => 'hour',
        'offset' => 3600,
        'bot_id' => (int) ($config['hour_bot_id'] ?? 0),
    ];
}

function zoom_salesbot_read_launch_state(int $leadId, array $config): ?array
{
    $path = zoom_salesbot_state_path($leadId, $config);
    if (!is_file($path)) {
        return null;
    }

    $state = json_decode((string) file_get_contents($path), true);
    return is_array($state) ? $state : null;
}

function zoom_salesbot_write_launch_state(array $event, array $config): void
{
    if (!is_dir($config['state_dir'])) {
        @mkdir($config['state_dir'], 0750, true);
    }

    $event['launched_at'] = time();
    file_put_contents(
        zoom_salesbot_state_path((int) $event['lead_id'], $config),
        json_encode($event, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        LOCK_EX
    );
}

function zoom_salesbot_capture_lead(array $lead, ?array $config = null): ?array
{
    $config ??= zoom_salesbot_config();
    if (!$config['enabled']) {
        return null;
    }
    $event = zoom_salesbot_start_event_from_lead($lead, $config);
    if ($event === null) {
        return null;
    }

    $previous = zoom_salesbot_read_launch_state($event['lead_id'], $config);
    if ((int) ($previous['start_at'] ?? 0) === $event['start_at']) {
        log_info('Salesbot Zoom не запущен: поле начала встречи не изменилось', [
            'lead_id' => $event['lead_id'],
            'start_at' => $event['start_at'],
        ], 'zoom_salesbot_reminders.php');
        return null;
    }

    $event['now_at'] = time();
    zoom_salesbot_write_launch_state($event, $config);
    zoom_salesbot_create_agents($event, $config);
    log_info('Агенты Salesbot Zoom созданы по новой дате начала встречи', [
        'lead_id' => $event['lead_id'],
        'start_at' => $event['start_at'],
        'previous_start_at' => $previous['start_at'] ?? null,
        'bot_id' => $config['bot_id'],
    ], 'zoom_salesbot_reminders.php');

    return $event;
}

function zoom_salesbot_create_agents(array $event, array $config): void
{
    $dir = rtrim($config['state_dir'], '/') . '/agents';
    if (!is_dir($dir) && !@mkdir($dir, 0750, true) && !is_dir($dir)) {
        throw new RuntimeException('Не удалось создать каталог агентов Salesbot.');
    }

    // A reschedule creates a new agent; old pending agents must never fire.
    foreach (glob($dir . '/agent_' . $event['lead_id'] . '_*.json') ?: [] as $oldPath) {
        $old = json_decode((string) @file_get_contents($oldPath), true);
        if (is_array($old) && ($old['status'] ?? '') === 'pending' && (int) ($old['start_at'] ?? 0) !== (int) $event['start_at']) {
            $old['status'] = 'cancelled';
            $old['cancelled_at'] = time();
            @file_put_contents($oldPath, json_encode($old, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES), LOCK_EX);
        }
    }

    $agentConfig = zoom_salesbot_agent_definition($event, $config);
    $kind = $agentConfig['kind'];
    $offset = $agentConfig['offset'];
    $agent = [
            'id' => $event['lead_id'] . ':' . $event['start_at'] . ':' . $kind,
            'lead_id' => $event['lead_id'],
            'start_at' => $event['start_at'],
            'run_at' => max(0, $event['start_at'] - $offset),
            'kind' => $kind,
            'bot_id' => $agentConfig['bot_id'],
            'status' => 'pending',
            'created_at' => time(),
    ];
    if ($agent['bot_id'] <= 0) {
        throw new RuntimeException('Не задан Salesbot для напоминания: ' . $kind);
    }
    file_put_contents($dir . '/agent_' . $event['lead_id'] . '_' . $event['start_at'] . '_' . $kind . '.json', json_encode($agent, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES), LOCK_EX);
}

function zoom_salesbot_run_agents(?int $now = null, ?array $config = null): array
{
    $config ??= zoom_salesbot_config();
    $now ??= time();
    $result = ['sent' => 0, 'skipped' => 0, 'errors' => 0];
    $dir = rtrim($config['state_dir'], '/') . '/agents';
    foreach (glob($dir . '/agent_*.json') ?: [] as $path) {
        $agent = json_decode((string) file_get_contents($path), true);
        if (!is_array($agent) || ($agent['status'] ?? '') !== 'pending' || (int) ($agent['run_at'] ?? 0) > $now) {
            $result['skipped']++;
            continue;
        }
        $lock = $path . '.lock';
        $fp = @fopen($lock, 'x');
        if ($fp === false) {
            $result['skipped']++;
            continue;
        }
        try {
            $agentConfig = $config;
            $agentConfig['bot_id'] = (int) ($agent['bot_id'] ?? 0);
            zoom_salesbot_run((int) $agent['lead_id'], $agentConfig);
            $agent['status'] = 'sent';
            $agent['sent_at'] = time();
            file_put_contents($path, json_encode($agent, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES), LOCK_EX);
            $result['sent']++;
        } catch (Throwable $exception) {
            log_error('Ошибка агента Salesbot Zoom', ['agent' => $agent['id'], 'error' => $exception->getMessage()], 'zoom_salesbot_reminders.php');
            $result['errors']++;
        } finally {
            fclose($fp);
            @unlink($lock);
        }
    }
    return $result;
}

function zoom_salesbot_run_due(?int $now = null, ?array $config = null): array
{
    $config ??= zoom_salesbot_config();
    $now ??= time();
    $result = ['sent' => 0, 'skipped' => 0, 'errors' => 0];
    foreach (glob(rtrim($config['state_dir'], '/') . '/lead_*.json') ?: [] as $path) {
        $schedule = json_decode((string) file_get_contents($path), true);
        if (!is_array($schedule)) {
            $result['errors']++;
            continue;
        }
        foreach ([['key' => 'day', 'offset' => 86400]] as $reminder) {
            $dueAt = (int) $schedule['start_at'] - $reminder['offset'];
            $stateKey = $reminder['key'] . ':' . $schedule['start_at'] . ':' . $schedule['end_at'];
            $sentPath = $path . '.' . $reminder['key'];
            if ($now < $dueAt || is_file($sentPath)) {
                $result['skipped']++;
                continue;
            }
            try {
                $leadId = (int) $schedule['lead_id'];
                $lead = fetchLeadData($leadId);
                $fresh = zoom_salesbot_schedule_from_lead($lead, $config);
                if ($fresh === null) {
                    // The meeting was cleared or moved out of the configured pipeline.
                    // Remove the stale schedule instead of retrying it forever.
                    @unlink($path);
                    $result['skipped']++;
                    continue;
                }
                if ($fresh['start_at'] !== (int) $schedule['start_at'] || $fresh['end_at'] !== (int) $schedule['end_at']) {
                    zoom_salesbot_save_schedule($fresh, $config);
                    $result['skipped']++;
                    continue;
                }
                zoom_salesbot_run($leadId, $config);
                file_put_contents($sentPath, $stateKey, LOCK_EX);
                $result['sent']++;
            } catch (Throwable $exception) {
                log_error('Ошибка запуска Salesbot-напоминания', ['lead_id' => $schedule['lead_id'], 'reminder' => $reminder['key'], 'error' => $exception->getMessage()], 'zoom_salesbot_reminders.php');
                $result['errors']++;
            }
        }
    }
    return $result;
}

function zoom_salesbot_run(int $leadId, array $config): array
{
    global $subdomain, $data;
    if ($config['bot_id'] <= 0) {
        throw new RuntimeException('Не задан ZOOM_SALESBOT_ID.');
    }
    $payload = ['entity_id' => $leadId, 'entity_type' => 'leads'];
    $response = post_or_patch($subdomain, $payload, '/api/v4/bots/' . $config['bot_id'] . '/run', $data, 'POST');
    if (!is_array($response)) {
        throw new RuntimeException('amoCRM не вернул корректный ответ при запуске Salesbot.');
    }
    log_info('Salesbot-напоминание запущено', ['lead_id' => $leadId, 'bot_id' => $config['bot_id']], 'zoom_salesbot_reminders.php');
    return $response;
}

if (PHP_SAPI === 'cli' && in_array('--run', $argv ?? [], true)) {
    try {
        $runResult = zoom_salesbot_run_due();
        log_info('Cron Salesbot-напоминаний завершен', $runResult, 'zoom_salesbot_reminders.php');
        echo json_encode($runResult, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . PHP_EOL;
    } catch (Throwable $exception) {
        log_error('Критическая ошибка cron Salesbot-напоминаний', ['error' => $exception->getMessage()], 'zoom_salesbot_reminders.php');
        fwrite(STDERR, $exception->getMessage() . PHP_EOL);
        exit(1);
    }
}
