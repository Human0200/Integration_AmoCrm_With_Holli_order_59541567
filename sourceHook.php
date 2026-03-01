<?php
require_once('./logger.php');
require_once('./amo_func.php');

// --- Конфигурация ---
$salebot_api_key = '35fa1d3f223b1be010e3f95bf0fc5e44';
$subdomain = 'directorchinatutorru';
$target_field_id = 1598377; // ID поля "Канал"

$enum_map = [
    'Instagram' => 7557063,  // insta SB
    'VK' => 7557061,         // vk SB
    'Telegram' => 7557065,   // TG SB
    'WhatsApp' => 7557067,   // WA SB
    'Онлайн-чат' => 7557069, // Онлайн-чат
    'MAX' => 7557059         // max SB
];

// Маппинг client_type для тех же значений
$client_type_map = [
    '10' => 7557063, // Instagram -> insta SB
    '0' => 7557061,  // VK -> vk SB
    '1' => 7557065,  // Telegram -> TG SB
    '6' => 7557067,  // WhatsApp -> WA SB
    '4' => 7557059,  // Viber -> max SB (нет отдельного)
    '8' => 7557059,  // Facebook -> max SB (нет отдельного)
    '13' => 7557059  // Телефония -> max SB (нет отдельного)
];

// ДАННЫЕ ПРИХОДЯТ В $_POST
$webhook_data = $_POST;

log_message('DATA: ', $webhook_data, 'sourceHook.php');

if (empty($webhook_data)) {
    log_message('ERROR: Пустой $_POST', [], 'sourceHook.php');
    http_response_code(200);
    echo "OK - empty POST";
    exit;
}

// Получаем ID сделки
$leadId = null;

if (isset($webhook_data["leads"]["update"][0]["id"])) {
    $leadId = (int) $webhook_data["leads"]["update"][0]["id"];
}

if (!$leadId) {
    log_message('ERROR: Не удалось определить lead_id', $webhook_data, 'sourceHook.php');
    http_response_code(200);
    echo "OK - no lead ID";
    exit;
}

// Получаем название сделки
$lead_name = $webhook_data["leads"]["update"][0]["name"] ?? '';

// --- Извлекаем client_id из названия сделки ---
preg_match('/№(\d+)/', $lead_name, $matches);

if (empty($matches)) {
    log_message('ERROR: Не удалось найти client_id в названии сделки: ' . $lead_name, [], 'sourceHook.php');
    http_response_code(200);
    echo "OK - no client_id found";
    exit;
}

$salebot_client_id = $matches[1];

// --- ПОЛУЧАЕМ ТЕКУЩЕЕ ЗНАЧЕНИЕ ПОЛЯ "КАНАЛ" ---
$current_enum_id = null;

try {
    $lead_info = get($subdomain, "/api/v4/leads/{$leadId}", $GLOBALS['data']);
    
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
    
    if (!$current_enum_id) {
    }
    
} catch (Exception $e) {
    log_message('ERROR: Ошибка при получении данных сделки', [
        'error' => $e->getMessage(),
        'lead_id' => $leadId
    ], 'sourceHook.php');
}

// --- Запрашиваем переменные клиента из Salebot ---
$salebot_url = "https://chatter.salebot.pro/api/{$salebot_api_key}/get_variables?client_id={$salebot_client_id}";


$ch = curl_init();
curl_setopt($ch, CURLOPT_URL, $salebot_url);
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
curl_setopt($ch, CURLOPT_TIMEOUT, 10);

$salebot_response = curl_exec($ch);
$http_code = curl_getinfo($ch, CURLINFO_HTTP_CODE);

if (curl_error($ch)) {
    log_message('ERROR: Ошибка cURL при запросе к Salebot: ' . curl_error($ch), [], 'sourceHook.php');
    curl_close($ch);
    http_response_code(200);
    echo "OK - curl error";
    exit;
}

curl_close($ch);


if ($http_code != 200) {
    log_message('ERROR: Salebot вернул ошибку HTTP ' . $http_code, [], 'sourceHook.php');
    http_response_code(200);
    echo "OK - salebot error";
    exit;
}

// Декодируем ответ Salebot
$salebot_data = json_decode($salebot_response, true);

if (!$salebot_data) {
    log_message('ERROR: Не удалось декодировать ответ Salebot', ['response' => $salebot_response], 'sourceHook.php');
    http_response_code(200);
    echo "OK - invalid salebot response";
    exit;
}

// --- ОПРЕДЕЛЯЕМ НОВОЕ ЗНАЧЕНИЕ ИЗ SALEBOT ---
$new_enum_id = null;

// Сначала пробуем получить messenger
if (isset($salebot_data['messenger']) && !empty($salebot_data['messenger'])) {
    $messenger_value = $salebot_data['messenger'];
    $new_enum_id = $enum_map[$messenger_value] ?? null;
}

// Если не нашли по messenger, пробуем по client_type
if (!$new_enum_id && isset($salebot_data['client_type'])) {
    $client_type = (string)$salebot_data['client_type'];
    $new_enum_id = $client_type_map[$client_type] ?? null;
}

if (!$new_enum_id) {
    log_message('WARNING: Не удалось определить канал, ставим max SB', [
        'messenger' => $salebot_data['messenger'] ?? 'не указан',
        'client_type' => $salebot_data['client_type'] ?? 'не указан'
    ], 'sourceHook.php');
    
    $new_enum_id = 7557059; // max SB по умолчанию
}



// --- ПРОВЕРЯЕМ, НУЖНО ЛИ ОБНОВЛЯТЬ ---
if ($current_enum_id === $new_enum_id) {
    http_response_code(200);
    echo "OK - no update needed";
    exit;
}

// --- Обновляем сделку ---
$leadsData = [
    'id' => $leadId,
    'custom_fields_values' => [
        [
            'field_id' => $target_field_id,
            'values' => [
                [
                    'enum_id' => $new_enum_id
                ]
            ]
        ]
    ]
];


try {
    $result = post_or_patch(
        $subdomain,
        $leadsData,
        "/api/v4/leads/{$leadId}",
        $GLOBALS['data'],
        'PATCH'
    );
    
    log_message('SUCCESS: Поле "Канал" обновлено', ['result' => $result], 'sourceHook.php');
    
} catch (Exception $e) {
    log_message('ERROR: Ошибка при обновлении', [
        'error' => $e->getMessage(),
        'lead_id' => $leadId
    ], 'sourceHook.php');
}

http_response_code(200);
echo "OK - completed";
?>