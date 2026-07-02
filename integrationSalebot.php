<?php
require_once('./amo_func.php');

// --- Конфигурация ---
$subdomain = 'directorchinatutorru';
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
$amo_unsorted_id = $salebot_data['client']['variables']['amo_unsorted_id']
    ?? $salebot_data['client']['order_variables']['amo_unsorted_id']
    ?? null;
$amo_client_id = $salebot_data['client']['variables']['amo_client_id']
    ?? $salebot_data['client']['order_variables']['amo_client_id']
    ?? null;
$amo_lead_id = $salebot_data['client']['order_variables']['amo_lead_id']
    ?? $salebot_data['client']['variables']['amo_lead_id']
    ?? null;
$client_type = $salebot_data['client']['client_type'] ?? null;

// Также проверяем поле messenger если есть
$messenger = $salebot_data['client']['variables']['messenger'] ?? null;

// Извлекаем UTM-метки и viewed_page из переменных Salebot
$utm_values = [];
foreach (array_keys($utm_field_map) as $utm_key) {
    $val = $salebot_data['client']['variables'][$utm_key] ?? null;
    if (!empty($val)) {
        $utm_values[$utm_key] = $val;
    }
}

write_log('ИЗВЛЕЧЕННЫЕ ДАННЫЕ', [
    'amo_unsorted_id' => $amo_unsorted_id,
    'amo_client_id' => $amo_client_id,
    'amo_lead_id' => $amo_lead_id,
    'client_type' => $client_type,
    'messenger' => $messenger,
    'utm_values' => $utm_values,
]);

if (!$amo_unsorted_id && !$amo_client_id && !$amo_lead_id) {
    write_log('ОШИБКА: Нет ни одного идентификатора');
    http_response_code(200);
    echo "OK - no identifiers";
    exit;
}

$lead_id = null;
$found_by = null;

// --- 1. Пробуем найти по amo_lead_id ---
if ($amo_lead_id) {
    write_log('ПОИСК ПО amo_lead_id: ' . $amo_lead_id);
    
    try {
        $lead_data = get($subdomain, "/api/v4/leads/{$amo_lead_id}", $GLOBALS['data']);
        if ($lead_data && isset($lead_data['id'])) {
            $lead_id = $lead_data['id'];
            $found_by = 'lead_id';
            write_log('НАЙДЕНО через lead_id', ['lead_id' => $lead_id]);
        }
    } catch (Exception $e) {
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
if (!$lead_id && $amo_client_id) {
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

if (!$lead_id) {
    write_log('ОШИБКА: Сделка не найдена ни по одному идентификатору');
    http_response_code(200);
    echo "OK - lead not found";
    exit;
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

write_log('ПОЛЯ ДЛЯ ОБНОВЛЕНИЯ', [
    'channel_changed' => ($current_enum_id !== $enum_id),
    'utm_values'      => $utm_values,
    'fields_count'    => count($custom_fields),
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

http_response_code(200);
echo "OK - completed";
?>