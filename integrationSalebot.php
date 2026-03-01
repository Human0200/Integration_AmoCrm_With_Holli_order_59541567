<?php
require_once('./amo_func.php');

// --- Конфигурация ---
$subdomain = 'directorchinatutorru';
$target_field_id = 1598377; // ID поля "Канал"
$log_file = __DIR__ . '/logs/salebot_debug.txt'; // Файл для логов
// --------------------

// Функция для записи в лог
function write_log($message, $data = [], $debug = true) {
    if($debug == true){
    global $log_file;
    $timestamp = date('Y-m-d H:i:s');
    $log_entry = "[{$timestamp}] {$message}\n";
    if (!empty($data)) {
        $log_entry .= print_r($data, true) . "\n";
    }
    $log_entry .= "----------------------------------------\n";
    file_put_contents($log_file, $log_entry, FILE_APPEND);
}
}

// Получаем данные из вебхука Salebot
$inputJSON = file_get_contents('php://input');
$salebot_data = json_decode($inputJSON, true);

write_log('SALEBOT WEBHOOK ПОЛУЧЕН', $salebot_data);

if (!$salebot_data) {
    write_log('ОШИБКА: Не удалось декодировать JSON', ['input' => substr($inputJSON, 0, 500)]);
    http_response_code(200);
    echo "OK - invalid JSON";
    exit;
}

// Извлекаем данные из вебхука
$amo_unsorted_id = $salebot_data['client']['variables']['amo_unsorted_id'] ?? null;
$amo_client_id = $salebot_data['client']['variables']['amo_client_id'] ?? null;
$client_type = $salebot_data['client']['client_type'] ?? null;

write_log('ИЗВЛЕЧЕННЫЕ ДАННЫЕ', [
    'amo_unsorted_id' => $amo_unsorted_id,
    'amo_client_id' => $amo_client_id,
    'client_type' => $client_type
]);

if (!$amo_unsorted_id && !$amo_client_id) {
    write_log('ОШИБКА: Нет ни одного идентификатора');
    http_response_code(200);
    echo "OK - no identifiers";
    exit;
}

$lead_id = null;
$found_by = null;

// --- 1. Пробуем найти по amo_unsorted_id ---
if ($amo_unsorted_id) {
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

// --- 2. Если не нашли по unsorted_id, пробуем по amo_client_id ---
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

// --- 3. Если не нашли ---
if (!$lead_id) {
    write_log('ОШИБКА: Сделка не найдена ни по одному идентификатору');
    http_response_code(200);
    echo "OK - lead not found";
    exit;
}

// --- 4. Определяем канал ---
if (!$client_type) {
    write_log('ОШИБКА: Нет client_type');
    http_response_code(200);
    echo "OK - no client_type";
    exit;
}

$client_type_map = [
    '0' => 7557061,  // VK
    '1' => 7557065,  // Telegram
    '4' => 7557059,  // Viber -> max SB
    '6' => 7557067,  // WhatsApp
    '8' => 7557059,  // Facebook -> max SB
    '10' => 7557063, // Instagram
    '13' => 7557059  // Телефония -> max SB
];

$enum_id = $client_type_map[$client_type] ?? 7557059;
write_log('ОПРЕДЕЛЕН КАНАЛ', ['client_type' => $client_type, 'enum_id' => $enum_id]);

// --- 5. Получаем текущее значение ---
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

// --- 6. Обновляем если нужно ---
if ($current_enum_id === $enum_id) {
    write_log('ОБНОВЛЕНИЕ НЕ ТРЕБУЕТСЯ - значение уже правильное', [
        'lead_id' => $lead_id,
        'enum_id' => $enum_id
    ]);
} else {
    $leadsData = [
        'id' => $lead_id,
        'custom_fields_values' => [
            [
                'field_id' => $target_field_id,
                'values' => [
                    ['enum_id' => $enum_id]
                ]
            ]
        ]
    ];
    
    try {
        $result = post_or_patch(
            $subdomain,
            $leadsData,
            "/api/v4/leads/{$lead_id}",
            $GLOBALS['data'],
            'PATCH'
        );
        
        write_log('УСПЕХ: Поле обновлено', [
            'lead_id' => $lead_id,
            'old_value' => $current_enum_id,
            'new_value' => $enum_id,
            'found_by' => $found_by
        ]);
        
    } catch (Exception $e) {
        write_log('ОШИБКА при обновлении', [
            'error' => $e->getMessage(),
            'lead_id' => $lead_id
        ]);
    }
}

http_response_code(200);
echo "OK - completed";
?>