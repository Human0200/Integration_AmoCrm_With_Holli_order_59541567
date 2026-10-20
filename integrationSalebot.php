<?php
require_once('./amo_func.php');
require_once __DIR__ . '/salebot_amocrm_chat_bridge.php';

// --- Конфигурация ---
$subdomain = 'directorchinatutorru';
$new_lead_pipeline_id = (int) (getenv('SALEBOT_NEW_LEAD_PIPELINE_ID') ?: 7909930);
$new_lead_status_id = (int) (getenv('SALEBOT_NEW_LEAD_STATUS_ID') ?: 78567802);
$salebot_api_key = trim((string) (getenv('SALEBOT_API_KEY') ?: '35fa1d3f223b1be010e3f95bf0fc5e44'));
$target_field_id = 1598377; // ID поля "Канал"
$log_file = __DIR__ . '/logs/salebot_debug.txt';

// Маппинг Salebot-переменных → field_id в AmoCRM
// Каждая переменная пишется сразу в несколько полей (старые UTM + ДКТ)
$utm_field_map = [
    'utm_source'   => [1138341, 1623791], // utm_source   + ДКТ: Источник
    'utm_medium'   => [1138337, 1623793], // utm_medium   + ДКТ: Канал
    'utm_campaign' => [1138339, 1623795], // utm_campaign + ДКТ: Кампания
    'utm_content'  => [1138335, 1623797], // utm_content  + ДКТ: Содержимое
    'utm_term'     => [1138343, 1623799], // utm_term     + ДКТ: Что искал
    'viewed_page'  => [1623809],          // ДКТ: Страница звонка
];

// Only the customer's birth date is also used by the CRM workflow. Technical
// Salebot identifiers are kept in the note/chat bridge, not in custom fields.
$salebot_field_map = [
    'bdate' => ['field_id' => 1598451, 'type' => 'date'],
];
// --------------------

// Правильный маппинг значений
$enum_map = [
    'Instagram' => 7557063,  // insta SB
    'VK' => 7557061,         // vk SB
    'Telegram' => 7557065,   // TG SB
    'WhatsApp' => 7557067,   // WA SB
    'Онлайн-чат' => 7557069, // Онлайн-чат
    'MAX' => 7557059         // max SB
];

// Маппинг client_type в названия каналов
$client_type_to_name = [
    '0' => 'VK',
    '1' => 'Telegram',
    '4' => 'MAX',      // Viber -> max SB
    '5' => 'Онлайн-чат',
    '6' => 'WhatsApp',
    '8' => 'MAX',      // Facebook -> max SB
    '10' => 'Instagram',
    '13' => 'MAX',     // Телефония -> max SB
    '20' => 'MAX',     // TamTam -> max SB
];

function write_log($message, $data = []) {
    global $log_file;
    $timestamp = date('Y-m-d H:i:s');
    $log_entry = "[{$timestamp}] {$message}\n";
    if (!empty($data)) {
        $log_entry .= print_r($data, true) . "\n";
    }
    $log_entry .= "----------------------------------------\n";
    file_put_contents($log_file, $log_entry, FILE_APPEND);
}

/**
 * Request Salebot without using phone/email as the chat identity.
 * Salebot documents client_id as the stable client identifier and
 * platform_id + group_id as the way to restore it from a messenger chat.
 */
function salebot_api_request(string $apiKey, string $path, array $payload = [], string $method = 'GET'): array {
    if ($apiKey === '') {
        return ['ok' => false, 'status' => 0, 'data' => null];
    }

    $url = 'https://chatter.salebot.pro/api/' . rawurlencode($apiKey) . '/' . ltrim($path, '/');
    if ($method === 'GET' && $payload) {
        $url .= '?' . http_build_query($payload);
    }

    $ch = curl_init($url);
    $options = [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT => 15,
        CURLOPT_HTTPHEADER => ['Accept: application/json'],
    ];
    if ($method !== 'GET') {
        $options[CURLOPT_POST] = true;
        $options[CURLOPT_HTTPHEADER] = ['Accept: application/json', 'Content-Type: application/json'];
        $options[CURLOPT_POSTFIELDS] = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    }
    curl_setopt_array($ch, $options);
    $body = curl_exec($ch);
    $error = curl_error($ch);
    $status = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);

    $decoded = is_string($body) ? json_decode($body, true) : null;
    return [
        'ok' => $error === '' && $status >= 200 && $status < 300 && is_array($decoded),
        'status' => $status,
        'data' => is_array($decoded) ? $decoded : null,
    ];
}

function salebot_find_client_by_platform(string $apiKey, $platformId, $groupId): ?string {
    if ($apiKey === '' || $platformId === null || $platformId === '' || $groupId === null || $groupId === '') {
        return null;
    }

    // Salebot expects platform_ids as an array in a JSON object.
    $url = 'https://chatter.salebot.pro/api/' . rawurlencode($apiKey) . '/find_client_id_by_platform_id';
    $payload = [
        'platform_ids' => [(string) $platformId],
        'group_id' => $groupId,
    ];
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_POST => true,
        CURLOPT_HTTPHEADER => ['Accept: application/json', 'Content-Type: application/json'],
        CURLOPT_POSTFIELDS => json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        CURLOPT_TIMEOUT => 15,
    ]);
    $body = curl_exec($ch);
    $error = curl_error($ch);
    $status = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);

    $decoded = is_string($body) ? json_decode($body, true) : null;
    $item = is_array($decoded) && isset($decoded[0]) ? $decoded[0] : null;
    write_log('ПОИСК КЛИЕНТА SALEBOT ПО ЧАТУ', [
        'http_status' => $status,
        'curl_error' => $error ?: null,
        'found' => is_array($item) && !empty($item['id']),
        'platform_id_present' => true,
        'group_id_present' => true,
    ]);
    return is_array($item) && !empty($item['id']) ? (string) $item['id'] : null;
}

function salebot_restore_client_id(string $apiKey, $clientId, $platformId, $groupId): ?string {
    if ($clientId !== null && $clientId !== '') {
        return (string) $clientId;
    }
    if ($platformId === null || $platformId === '' || $groupId === null || $groupId === '') {
        return null;
    }

    return salebot_find_client_by_platform($apiKey, $platformId, $groupId);
}

function salebot_get_client_variables(string $apiKey, ?string $clientId): array {
    if (!$clientId) return [];
    $result = salebot_api_request($apiKey, 'get_variables', ['client_id' => $clientId]);
    if (!$result['ok']) return [];
    $data = $result['data'] ?? [];
    return is_array($data['variables'] ?? null) ? $data['variables'] : $data;
}

function salebot_save_amo_ids(string $apiKey, $clientId, int $leadId, ?int $amoClientId): array {
    if ($apiKey === '' || !$clientId) {
        return ['saved' => false, 'reason' => $apiKey === '' ? 'api_key_missing' : 'salebot_client_id_missing'];
    }
    $payload = [
        'client_id' => (string)$clientId,
        'variables' => [
            'amo_lead_id' => (string)$leadId,
            'amo_client_id' => $amoClientId ? (string)$amoClientId : '',
            'client.amo_lead_id' => (string)$leadId,
            'client.amo_client_id' => $amoClientId ? (string)$amoClientId : '',
        ],
    ];
    $ch = curl_init('https://chatter.salebot.pro/api/' . rawurlencode($apiKey) . '/save_variables');
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_POST => true,
        CURLOPT_HTTPHEADER => ['Content-Type: application/json'],
        CURLOPT_POSTFIELDS => json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        CURLOPT_TIMEOUT => 15,
    ]);
    $body = curl_exec($ch);
    $error = curl_error($ch);
    $status = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    $decoded = is_string($body) ? json_decode($body, true) : null;
    return [
        'saved' => $status >= 200 && $status < 300 && $error === '',
        'http_status' => $status,
        'curl_error' => $error ?: null,
        'response_status' => is_array($decoded) ? ($decoded['status'] ?? null) : null,
    ];
}

function salebot_extract_client_link(array $payload, array $variables): ?string
{
    $candidates = [
        $variables['salebot_link'] ?? null,
        $variables['salebot_url'] ?? null,
        $variables['client_url'] ?? null,
        $variables['profile_url'] ?? null,
        $variables['link'] ?? null,
        $variables['url'] ?? null,
        $payload['client']['salebot_link'] ?? null,
        $payload['client']['url'] ?? null,
        $payload['client']['profile_url'] ?? null,
        $payload['client']['link'] ?? null,
    ];

    foreach ($candidates as $candidate) {
        $candidate = trim((string)$candidate);
        if ($candidate !== '' && filter_var($candidate, FILTER_VALIDATE_URL)) {
            return $candidate;
        }
    }

    return null;
}

function salebot_date_to_timestamp($value): ?int
{
    $value = trim((string)$value);
    if ($value === '') {
        return null;
    }

    foreach (['d.m.Y', 'Y-m-d', 'd/m/Y'] as $format) {
        $date = DateTime::createFromFormat('!' . $format, $value, new DateTimeZone('Europe/Moscow'));
        if ($date instanceof DateTime && $date->format($format) === $value) {
            return $date->setTime(0, 0, 0)->getTimestamp();
        }
    }

    $timestamp = strtotime($value);
    return $timestamp === false ? null : $timestamp;
}

function salebot_build_extra_fields(?array $variables, ?string $clientId, ?string $platformId, ?string $groupId, ?string $amoClientId): array
{
    global $salebot_field_map;
    if (!is_array($salebot_field_map ?? null)) {
        $salebot_field_map = [
            'bdate' => ['field_id' => 1598451, 'type' => 'date'],
        ];
    }
    $variables = is_array($variables) ? $variables : [];
    $values = [
        'amo_client_id' => $clientId,
        'platform_id' => $platformId,
        'group_id' => $groupId,
        'domain' => $variables['domain'] ?? null,
        'sex' => $variables['sex'] ?? null,
        'cmid' => $variables['cmid'] ?? null,
        'bdate' => $variables['bdate'] ?? null,
    ];
    $fields = [];

    foreach ($values as $key => $value) {
        if ($value === null || $value === '') {
            continue;
        }
        $config = $salebot_field_map[$key] ?? null;
        if (!$config) {
            continue;
        }
        if ($config['type'] === 'date') {
            $value = salebot_date_to_timestamp($value);
            if ($value === null) {
                continue;
            }
        }
        $fieldValue = $config['type'] === 'date' || $config['type'] === 'numeric'
            ? (int)$value
            : (string)$value;
        $fields[] = [
            'field_id' => (int)$config['field_id'],
            'values' => [['value' => $fieldValue]],
        ];
    }

    return $fields;
}

function salebot_add_lead_note(string $subdomain, array $data, int $leadId, array $payload, array $variables, ?string $clientLink): array
{
    $client = is_array($payload['client'] ?? null) ? $payload['client'] : [];
    $lines = ['Salebot: клиент создан или синхронизирован'];

    if ($clientLink !== null) {
        $lines[] = 'Ссылка клиента: ' . $clientLink;
    } else {
        $lines[] = 'Публичная ссылка Salebot не передана API; идентификаторы указаны ниже.';
    }

    $identifiers = [
        'client_id' => $client['id'] ?? $client['client_id'] ?? null,
        'platform_id' => $variables['platform_id'] ?? $client['platform_id'] ?? $client['recipient'] ?? $client['recepient'] ?? null,
        'client_type' => $client['client_type'] ?? null,
        'group' => $client['group'] ?? null,
    ];

    foreach ($identifiers as $name => $value) {
        if ($value !== null && $value !== '') {
            $lines[] = $name . ': ' . (string)$value;
        }
    }

    try {
        post_or_patch($subdomain, [[
            'entity_id' => $leadId,
            'note_type' => 'common',
            'params' => ['text' => implode("\n", $lines)],
        ]], '/api/v4/leads/notes', $data, 'POST');

        return ['saved' => true, 'has_link' => $clientLink !== null];
    } catch (Throwable $e) {
        write_log('Не удалось добавить данные Salebot в историю сделки', [
            'lead_id' => $leadId,
            'error' => $e->getMessage(),
        ]);
        return ['saved' => false, 'has_link' => $clientLink !== null, 'error' => $e->getMessage()];
    }
}

$inputJSON = file_get_contents('php://input');
$salebot_data = json_decode($inputJSON, true);

write_log('SALEBOT WEBHOOK ПОЛУЧЕН', $salebot_data);

if (!$salebot_data) {
    write_log('ОШИБКА: Не удалось декодировать JSON');
    http_response_code(200);
    echo "OK - invalid JSON";
    exit;
}

// Извлекаем данные (ищем в variables и order_variables)
$variables = $salebot_data['client']['variables'] ?? [];
$order_variables = $salebot_data['client']['order_variables'] ?? [];
$platform_id = $variables['platform_id']
    ?? $variables['vk_id']
    ?? $salebot_data['client']['platform_id']
    ?? $salebot_data['client']['recipient']
    ?? $salebot_data['client']['recepient']
    ?? null;
$group_id = $variables['group_id']
    ?? $variables['group']
    ?? $salebot_data['client']['group_id']
    ?? $salebot_data['client']['group']
    ?? null;
$client_id = $salebot_data['client']['id'] ?? $salebot_data['client']['client_id'] ?? null;

// Restore the same Salebot chat before looking at phone/email.
$client_id = salebot_restore_client_id($salebot_api_key, $client_id, $platform_id, $group_id);
$saved_client_variables = salebot_get_client_variables($salebot_api_key, $client_id);
if ($saved_client_variables) {
    $variables = array_replace($saved_client_variables, $variables);
}
$amo_unsorted_id = $variables['amo_unsorted_id']
    ?? $salebot_data['client']['order_variables']['amo_unsorted_id']
    ?? null;
$amo_client_id = $variables['amo_client_id']
    ?? $salebot_data['client']['order_variables']['amo_client_id']
    ?? null;
$amo_lead_id = $order_variables['amo_lead_id']
    ?? $salebot_data['client']['variables']['amo_lead_id']
    ?? null;
$amo_conversation_ref_id = $variables['amo_conversation_ref_id'] ?? $order_variables['amo_conversation_ref_id'] ?? null;
$client_type = $salebot_data['client']['client_type'] ?? null;

// Также проверяем поле messenger если есть
$messenger = $variables['messenger'] ?? null;
$name = trim((string)($variables['name'] ?? $salebot_data['client']['name'] ?? 'Новый клиент'));
$phone = trim((string)($variables['phone'] ?? $variables['tel'] ?? $variables['telephone'] ?? $salebot_data['client']['phone'] ?? ''));
$email = trim((string)($variables['email'] ?? $salebot_data['client']['email'] ?? ''));
$salebot_client_link = salebot_extract_client_link($salebot_data, $variables);
$messenger_fields = salebot_messenger_fields($salebot_data, $variables);

// Извлекаем UTM-метки и viewed_page из переменных Salebot
$utm_values = [];
foreach (array_keys($utm_field_map) as $utm_key) {
    $val = $variables[$utm_key] ?? $order_variables[$utm_key] ?? null;
    if (!empty($val)) {
        $utm_values[$utm_key] = $val;
    }
}

$extra_custom_fields = salebot_build_extra_fields(
    $variables,
    $client_id !== null ? (string)$client_id : null,
    $platform_id !== null ? (string)$platform_id : null,
    $group_id !== null ? (string)$group_id : null,
    $amo_client_id !== null ? (string)$amo_client_id : null
);

write_log('ИЗВЛЕЧЕННЫЕ ДАННЫЕ', [
    'amo_unsorted_id' => $amo_unsorted_id,
    'amo_client_id' => $amo_client_id,
    'amo_lead_id' => $amo_lead_id,
    'amo_conversation_ref_id' => $amo_conversation_ref_id,
    'client_id' => $client_id,
    'chat_identity' => [
        'has_platform_id' => $platform_id !== null && $platform_id !== '',
        'has_group_id' => $group_id !== null && $group_id !== '',
        'restored_from_platform' => $client_id !== null && !empty($platform_id),
    ],
    'has_phone' => $phone !== '',
    'has_email' => $email !== '',
    'client_type' => $client_type,
    'messenger' => $messenger,
    'utm_values' => $utm_values,
]);


function salebot_normalize_phone(string $value): string { return preg_replace('/\D+/', '', $value) ?: ''; }
function salebot_contact_values(array $contact): array {
    $out = ['phone' => [], 'email' => []];
    foreach (($contact['custom_fields_values'] ?? []) as $field) {
        $code = strtoupper((string)($field['field_code'] ?? ''));
        foreach (($field['values'] ?? []) as $value) {
            $v = trim((string)($value['value'] ?? ''));
            if ($v !== '' && $code === 'PHONE') $out['phone'][] = salebot_normalize_phone($v);
            if ($v !== '' && $code === 'EMAIL') $out['email'][] = strtolower($v);
        }
    }
    return $out;
}
function salebot_messenger_fields(array $payload, array $variables): array {
    $client = $payload['client'] ?? [];
    $type = (string)($client['client_type'] ?? '');
    $order = $client['order_variables'] ?? [];
    $fields = [];
    $put = static function (int $id, $value) use (&$fields): void {
        $value = trim((string)$value);
        if ($value !== '' && $value !== '.') $fields[$id] = $value;
    };
    if ($type === '20') {
        $request = json_decode((string)($order['tt_request'] ?? $variables['tt_request'] ?? ''), true);
        $put(1630424, $variables['max_id'] ?? $request['chat_id'] ?? $client['recepient'] ?? null);
        $put(1631985, $variables['max_user_id'] ?? $variables['tamtam_user_id'] ?? $request['user']['user_id'] ?? null);
    }
    if ($type === '1') {
        $request = json_decode((string)($order['tg_request'] ?? $variables['tg_request'] ?? ''), true);
        $from = $request['message']['from'] ?? [];
        $username = $variables['tg_username'] ?? $from['username'] ?? null;
        $username = trim((string)$username);
        $put(1621101, $variables['telegram_id'] ?? $from['id'] ?? $client['recepient'] ?? null);
        $put(1629273, $username);
        $put(1630032, $variables['telegram_login'] ?? $username);
        $put(1632097, $variables['telegram_phone'] ?? $from['phone_number'] ?? null);
    }
    return $fields;
}
function salebot_sync_contact_messenger_fields(string $subdomain, array $data, int $contactId, array $fields): void {
    if (!$fields) return;
    $contact = get($subdomain, '/api/v4/contacts/' . $contactId, $data);
    $current = [];
    foreach (($contact['custom_fields_values'] ?? []) as $field) {
        $current[(int)$field['field_id']] = (string)($field['values'][0]['value'] ?? '');
    }
    $updates = [];
    foreach ($fields as $fieldId => $value) {
        if (($current[$fieldId] ?? '') !== $value) $updates[] = ['field_id' => $fieldId, 'values' => [['value' => $value]]];
    }
    if ($updates) post_or_patch($subdomain, ['id' => $contactId, 'custom_fields_values' => $updates], '/api/v4/contacts/' . $contactId, $data, 'PATCH');
}
function salebot_find_duplicate_contact(string $subdomain, array $data, string $phone, string $email): ?array {
    $queries = array_values(array_unique(array_filter([$phone, $email])));
    foreach ($queries as $query) {
        try {
            $response = get($subdomain, '/api/v4/contacts?query=' . rawurlencode($query) . '&with=leads&limit=50', $data);
            foreach (($response['_embedded']['contacts'] ?? []) as $contact) {
                $values = salebot_contact_values($contact);
                if ($phone !== '' && in_array($phone, $values['phone'], true)) return $contact;
                if ($email !== '' && in_array(strtolower($email), $values['email'], true)) return $contact;
            }
        } catch (Throwable $e) { write_log('Ошибка поиска дубля', ['error' => $e->getMessage()]); }
    }
    return null;
}
function salebot_create_or_find_contact(string $subdomain, array $data, string $name, string $phone, string $email): array {
    $existing = salebot_find_duplicate_contact($subdomain, $data, salebot_normalize_phone($phone), $email);
    if ($existing && !empty($existing['id'])) return [$existing['id'], 'duplicate'];
    $fields = [];
    if ($phone !== '') $fields[] = ['field_code' => 'PHONE', 'values' => [['value' => $phone, 'enum_code' => 'WORK']]];
    if ($email !== '') $fields[] = ['field_code' => 'EMAIL', 'values' => [['value' => $email, 'enum_code' => 'WORK']]];
    $created = post_or_patch($subdomain, [['name' => $name, 'custom_fields_values' => $fields]], '/api/v4/contacts', $data, 'POST');
    $id = $created['_embedded']['contacts'][0]['id'] ?? $created[0]['id'] ?? null;
    if (!$id) throw new RuntimeException('amoCRM не вернул ID созданного контакта');
    return [(int)$id, 'created'];
}

function salebot_resolve_lead_status(string $subdomain, array $data, int $pipelineId, int $configuredStatusId): int {
    try {
        $pipeline = get($subdomain, '/api/v4/leads/pipelines/' . $pipelineId, $data);
        $statuses = $pipeline['_embedded']['statuses'] ?? [];
        foreach ($statuses as $status) {
            if ((int)($status['id'] ?? 0) === $configuredStatusId && (int)($status['type'] ?? 0) === 0) {
                return $configuredStatusId;
            }
        }
        foreach ($statuses as $status) {
            if ((int)($status['type'] ?? 0) === 0 && !empty($status['id'])) {
                write_log('Указан системный или недоступный статус, выбран первый обычный этап', [
                    'pipeline_id' => $pipelineId,
                    'configured_status_id' => $configuredStatusId,
                    'resolved_status_id' => (int)$status['id'],
                ]);
                return (int)$status['id'];
            }
        }
    } catch (Throwable $e) {
        write_log('Не удалось проверить статус воронки', [
            'pipeline_id' => $pipelineId,
            'error' => $e->getMessage(),
        ]);
    }
    return $configuredStatusId;
}

$lead_id = null;
$found_by = null;
$explicit_lead_missing = false;

// --- 1. Пробуем найти по amo_lead_id ---
if ($amo_lead_id) {
    write_log('ПОИСК ПО amo_lead_id: ' . $amo_lead_id);
    
    try {
        $lead_data = get($subdomain, "/api/v4/leads/{$amo_lead_id}", $GLOBALS['data']);
        if ($lead_data && isset($lead_data['id'])) {
            $lead_id = $lead_data['id'];
            $found_by = 'lead_id';
            write_log('НАЙДЕНО через lead_id', ['lead_id' => $lead_id]);
        } else {
            // amoCRM returns 204 for a deleted/missing lead; do not fall back to an old deal of the same contact.
            $explicit_lead_missing = true;
            $amo_lead_id = null;
            write_log('Сделка не найдена по lead_id: пустой ответ API');
        }
    } catch (Exception $e) {
        $explicit_lead_missing = true;
        $amo_lead_id = null;
        write_log('Сделка не найдена по lead_id', ['error' => $e->getMessage()]);
    }
}

// --- 2. Пробуем найти по amo_unsorted_id ---
if (!$lead_id && $amo_unsorted_id) {
    write_log('ПОИСК ПО amo_unsorted_id: ' . $amo_unsorted_id);
    
    try {
        $unsorted_data = get($subdomain, "/api/v4/leads/unsorted/{$amo_unsorted_id}", $GLOBALS['data']);
        
        if ($unsorted_data && isset($unsorted_data['_embedded']['leads'][0]['id'])) {
            $lead_id = $unsorted_data['_embedded']['leads'][0]['id'];
            $found_by = 'unsorted_id';
            write_log('НАЙДЕНО через unsorted_id', ['lead_id' => $lead_id]);
        }
    } catch (Exception $e) {
        write_log('Неразобранное не найдено по UID', ['error' => $e->getMessage()]);
    }
}

// --- 3. Если не нашли, пробуем по amo_client_id ---
if (!$lead_id && $amo_client_id && !$explicit_lead_missing) {
    write_log('ПОИСК ПО amo_client_id: ' . $amo_client_id);
    
    try {
        $contact_data = get($subdomain, "/api/v4/contacts/{$amo_client_id}", $GLOBALS['data']);
        
        if ($contact_data) {
            write_log('Контакт найден, ищем сделки');
            
            $leads_data = get($subdomain, "/api/v4/leads?filter[contacts_id][]={$amo_client_id}&limit=1", $GLOBALS['data']);
            
            if (!empty($leads_data['_embedded']['leads'][0]['id'])) {
                $lead_id = $leads_data['_embedded']['leads'][0]['id'];
                $found_by = 'client_id';
                write_log('НАЙДЕНО через контакт', ['lead_id' => $lead_id]);
            } else {
                write_log('Сделок для контакта не найдено');
            }
        }
    } catch (Exception $e) {
        write_log('Контакт не найден', ['error' => $e->getMessage()]);
    }
}

// --- 4. Без идентификаторов ищем контакт по телефону/email и связанную сделку ---
if (!$lead_id && !$amo_unsorted_id && !$amo_lead_id) {
    write_log('Идентификаторы отсутствуют, выполняем поиск дубля по телефону/email');
    try {
        if (!$amo_client_id) {
            $contact = salebot_find_duplicate_contact($subdomain, $GLOBALS['data'], salebot_normalize_phone($phone), $email);
            if ($contact && !empty($contact['id'])) {
                $amo_client_id = (int)$contact['id'];
                $relatedLeads = $contact['_embedded']['leads'] ?? [];
                if (!$explicit_lead_missing && !empty($relatedLeads[0]['id'])) {
                    $lead_id = (int)$relatedLeads[0]['id'];
                    $found_by = 'duplicate_contact_lead';
                    write_log('НАЙДЕН ДУБЛЬ: переиспользуем связанную сделку', ['lead_id' => $lead_id, 'contact_id' => $amo_client_id]);
                }
            }
        }
        if (!$lead_id) {
            $contact_action = $amo_client_id ? 'existing_from_stale_lead_id' : null;
            if (!$amo_client_id) {
                [$amo_client_id, $contact_action] = salebot_create_or_find_contact($subdomain, $GLOBALS['data'], $name, $phone, $email);
            }
            $resolvedStatusId = salebot_resolve_lead_status(
                $subdomain,
                $GLOBALS['data'],
                $new_lead_pipeline_id,
                $new_lead_status_id
            );
            $leadPayload = [[
                'name' => $name,
                'pipeline_id' => $new_lead_pipeline_id,
                'status_id' => $resolvedStatusId,
                '_embedded' => ['contacts' => [['id' => (int)$amo_client_id]]],
            ]];
            $createdLead = post_or_patch($subdomain, $leadPayload, '/api/v4/leads', $GLOBALS['data'], 'POST');
            $lead_id = (int)($createdLead['_embedded']['leads'][0]['id'] ?? $createdLead[0]['id'] ?? 0);
            if (!$lead_id) throw new RuntimeException('amoCRM не вернул ID созданной сделки');
            $found_by = 'created';
            write_log('СОЗДАНА новая сделка', ['lead_id' => $lead_id, 'contact_id' => $amo_client_id, 'contact_action' => $contact_action]);
            $salebotNoteResult = salebot_add_lead_note(
                $subdomain,
                $GLOBALS['data'],
                $lead_id,
                $salebot_data,
                $variables,
                $salebot_client_link
            );
            write_log('Данные Salebot добавлены в историю новой сделки', $salebotNoteResult + ['lead_id' => $lead_id]);
        }
    } catch (Throwable $e) {
        write_log('ОШИБКА создания/поиска сделки без идентификаторов', ['error' => $e->getMessage()]);
        http_response_code(200);
        echo 'OK - creation failed';
        exit;
    }
}

$chatBridgeEvent = $salebot_data;
$telegram_nickname = $messenger_fields[1629273] ?? null;
if ($amo_client_id && $messenger_fields) {
    try {
        salebot_sync_contact_messenger_fields($subdomain, $GLOBALS['data'], (int)$amo_client_id, $messenger_fields);
    } catch (Throwable $e) {
        write_log('Ошибка обновления полей мессенджера контакта', ['contact_id' => $amo_client_id, 'error' => $e->getMessage()]);
    }
}
$chatBridgeEvent['client']['variables'] = $variables;
$chatBridgeResult = salebot_amocrm_bridge_process_salebot_event(
    $chatBridgeEvent,
    trim((string)(getenv('AMO_CHAT_SALEBOT_API_KEY') ?: '')),
    'directorchinatutorru',
    !empty($lead_id) ? (int)$lead_id : null,
    !empty($amo_client_id) ? (int)$amo_client_id : null
);
if (empty($chatBridgeResult['skipped'])) {
    write_log('Результат Salebot ↔ amoCRM chat bridge', [
        'ok' => $chatBridgeResult['ok'] ?? false,
        'step' => $chatBridgeResult['step'] ?? null,
        'status' => $chatBridgeResult['status'] ?? null,
        'chat_id' => $chatBridgeResult['chat_id'] ?? null,
    ]);
}

// --- 4. Определяем канал ---
$channel_name = null;

// Сначала пробуем по messenger если есть
if ($messenger && isset($enum_map[$messenger])) {
    $channel_name = $messenger;
    write_log('Канал определен через messenger', ['messenger' => $messenger]);
} 
// Иначе по client_type
elseif ($client_type !== null && isset($client_type_to_name[(string)$client_type])) {
    $channel_name = $client_type_to_name[(string)$client_type];
    write_log('Канал определен через client_type', [
        'client_type' => $client_type,
        'channel_name' => $channel_name
    ]);
}

if (!$channel_name) {
    write_log('ОШИБКА: Не удалось определить канал', [
        'client_type' => $client_type,
        'messenger' => $messenger
    ]);
    http_response_code(200);
    echo "OK - unknown channel";
    exit;
}

if (!$lead_id) {
    write_log('ОШИБКА: Сделка не найдена ни по одному идентификатору');
    http_response_code(200);
    echo "OK - lead not found";
    exit;
}

$enum_id = $enum_map[$channel_name] ?? 7557059; // MAX по умолчанию
write_log('ОПРЕДЕЛЕН КАНАЛ', [
    'channel_name' => $channel_name,
    'enum_id' => $enum_id,
    'found_by' => $found_by
]);

// --- 5. Получаем текущее значение канала ---
$current_enum_id = null;

try {
    $lead_info = get($subdomain, "/api/v4/leads/{$lead_id}", $GLOBALS['data']);

    if (isset($lead_info['custom_fields_values']) && is_array($lead_info['custom_fields_values'])) {
        foreach ($lead_info['custom_fields_values'] as $field) {
            if (($field['field_id'] ?? 0) == $target_field_id) {
                if (isset($field['values'][0]['enum_id'])) {
                    $current_enum_id = $field['values'][0]['enum_id'];
                }
                break;
            }
        }
    }
    write_log('ТЕКУЩЕЕ ЗНАЧЕНИЕ ПОЛЯ', ['current_enum_id' => $current_enum_id]);

} catch (Exception $e) {
    write_log('Не удалось получить текущее значение', ['error' => $e->getMessage()]);
}

// --- 6. Формируем и отправляем обновление ---
$custom_fields = [];

// Канал — обновляем только если изменился
if ($current_enum_id !== $enum_id) {
    $custom_fields[] = [
        'field_id' => $target_field_id,
        'values'   => [['enum_id' => $enum_id]],
    ];
} else {
    write_log('Канал не изменился, пропускаем', ['enum_id' => $enum_id]);
}

// UTM-метки — пишем в каждое поле из маппинга
foreach ($utm_values as $utm_key => $utm_val) {
    foreach ($utm_field_map[$utm_key] as $field_id) {
        $custom_fields[] = [
            'field_id' => $field_id,
            'values'   => [['value' => $utm_val]],
        ];
    }
}

// Preserve Salebot chat/profile values on the lead for later reconciliation.
$custom_fields = array_merge($custom_fields, $extra_custom_fields);
if ($telegram_nickname !== null) {
    $custom_fields[] = ['field_id' => 1640403, 'values' => [['value' => $telegram_nickname]]];
}

write_log('ПОЛЯ ДЛЯ ОБНОВЛЕНИЯ', [
    'channel_changed' => ($current_enum_id !== $enum_id),
    'utm_values'      => $utm_values,
    'fields_count'    => count($custom_fields),
    'extra_fields_count' => count($extra_custom_fields),
]);

if (empty($custom_fields)) {
    write_log('Нечего обновлять — канал совпадает, UTM отсутствуют');
} else {
    $leadsData = [
        'id'                  => $lead_id,
        'custom_fields_values' => $custom_fields,
    ];

    try {
        $result = post_or_patch(
            $subdomain,
            $leadsData,
            "/api/v4/leads/{$lead_id}",
            $GLOBALS['data'],
            'PATCH'
        );

        write_log('УСПЕХ: Сделка обновлена', [
            'lead_id'      => $lead_id,
            'channel_name' => $channel_name,
            'utm_values'   => $utm_values,
            'found_by'     => $found_by,
        ]);

    } catch (Exception $e) {
        write_log('ОШИБКА при обновлении', [
            'error'   => $e->getMessage(),
            'lead_id' => $lead_id,
        ]);
    }
}

if ($client_id && $lead_id) {
    $salebotSaveResult = salebot_save_amo_ids($salebot_api_key, $client_id, (int)$lead_id, $amo_client_id ? (int)$amo_client_id : null);
    write_log('Обратная запись amo ID в Salebot', $salebotSaveResult + ['client_id_present' => !empty($client_id), 'lead_id' => (int)$lead_id]);
} else {
    $salebotSaveResult = ['saved' => false, 'reason' => 'required_client_or_lead_id_missing'];
    write_log('Обратная запись в Salebot пропущена', ['client_id_present' => !empty($client_id), 'lead_id' => (int)$lead_id]);
}

http_response_code(200);
header('Content-Type: application/json; charset=utf-8');
echo json_encode([
    'success' => true,
    'amo_lead_id' => (int)$lead_id,
    'amo_client_id' => $amo_client_id ? (int)$amo_client_id : null,
    'salebot_ids_saved' => $salebotSaveResult['saved'] ?? false,
    'chat_bridge' => $chatBridgeResult,
], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
?>
