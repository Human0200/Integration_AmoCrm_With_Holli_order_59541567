<?php

declare(strict_types=1);

require_once __DIR__ . '/logger.php';
require_once __DIR__ . '/config.php';
require_once __DIR__ . '/amo_func.php';

const HANDLER_SOURCE = 'cross_portal_closed_status_handler.php';
const HANDLER_LOG_FILE = __DIR__ . '/logs/cross_portal_closed_status_handler.log';

// Источник (другой портал) - токен, который вы прислали.
const SOURCE_AMO_ACCESS_TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6ImM2MDc4MDY1ZTczN2IwOWU1MTcxMDY5ZjUzZmIyN2QwNzBjMTA3YzIyODI0NzAxNzAzNjdhZGI2NzlhM2MwOWEyNzM1NGI5ZDk5ZjE1ZDMzIn0.eyJhdWQiOiIwNTQ0ZDY3NC00MmI4LTRiYTMtOTBlZC02OTM4MGFkMzNjNWQiLCJqdGkiOiJjNjA3ODA2NWU3MzdiMDllNTE3MTA2OWY1M2ZiMjdkMDcwYzEwN2MyMjgyNDcwMTcwMzY3YWRiNjc5YTNjMDlhMjczNTRiOWQ5OWYxNWQzMyIsImlhdCI6MTc3NTY2NDY0NSwibmJmIjoxNzc1NjY0NjQ1LCJleHAiOjE3ODAxODU2MDAsInN1YiI6IjEyNjQyMzE0IiwiZ3JhbnRfdHlwZSI6IiIsImFjY291bnRfaWQiOjMyNDk1NjI2LCJiYXNlX2RvbWFpbiI6ImFtb2NybS5ydSIsInZlcnNpb24iOjIsInNjb3BlcyI6WyJwdXNoX25vdGlmaWNhdGlvbnMiLCJmaWxlcyIsImNybSIsImZpbGVzX2RlbGV0ZSIsIm5vdGlmaWNhdGlvbnMiXSwiaGFzaF91dWlkIjoiMGFiYmNhYWUtYzUzNi00NzMwLWEyODAtNDdjNjAwMjBkMGJmIiwiYXBpX2RvbWFpbiI6ImFwaS1iLmFtb2NybS5ydSJ9.GWXm7Z4v0eXQcb9w3SrlTdeY31GGzJf4Ta7EZZune6O61j520bu-rQKyLeUhJNjZz4tPDLBbf7l5_P-60QGAg_aW-JwCwExpMb04_0FLZIl4GcclB_dnT9zoccvsrXe58bNl50du4hiAhan58GWb51K9zM0BI8A0cOB9Hasytno1dJ_eLF9euyYlP5d_yRqV-5TsryUCg6PDwQaKgfZsjOh-bHY60vog6NHuM5u66BN6l_4XI44tTARWYDySRx9UpwnsQLkCgDUfPKHE9ij4D92-hJJSkUubn9jygblVasxoOz0oCH65Rx38Jo2wHuZ21RjgGXOp-Dwb5gBOW3zoxA';
const SOURCE_AMO_BASE_URL = 'https://supportchinatutorru.amocrm.ru';

// Целевой портал (текущий, где уже работают интеграции через amo_func.php).
// Целевая воронка и единая стадия для переноса.
const TARGET_AMO_PIPELINE_ID = 9919562; //БАЗА ПРОГРЕВ КУРСЫ
const TARGET_AMO_STATUS_ID = 84476154; //ВЫБЫВШИЕ

// Маппинг полей сделки: source_field_id => настройки переноса в target.
// Если ID в обоих порталах одинаковые, просто оставьте target_field_id как source.
const TARGET_LEAD_FIELD_MAP = [
    1054565 => ['target_field_id' => 1604231, 'mode' => 'value'], // Филиал
    1054615 => ['target_field_id' => 1575217, 'mode' => 'enum_by_value'], // Язык
    1054623 => [
        'target_field_id' => 1618692,
        'mode' => 'enum_text_map',
        'text_map' => [
            'действующий' => 'да',
            'не действующий' => 'нет',
            'недействующий' => 'нет',
            'новый' => 'нет',
        ],
    ], // Статус ученика -> Действующий ученик
    1054625 => ['target_field_id' => 1575317, 'mode' => 'enum_by_value'], // Вид уроков
    1649627 => ['target_field_id' => 1598451, 'mode' => 'value'], // Дата рождения
    1698261 => ['target_field_id' => 1576357, 'mode' => 'enum_by_value'], // Уровень
    1060101 => ['target_field_id' => 1630807, 'mode' => 'value'], // Ссылка на ХХ -> Ссылка на Холи
    1050081 => ['target_field_id' => 1138335, 'mode' => 'value'], // utm_content
    1050083 => ['target_field_id' => 1138337, 'mode' => 'value'], // utm_medium
    1050085 => ['target_field_id' => 1138339, 'mode' => 'value'], // utm_campaign
    1050087 => ['target_field_id' => 1138341, 'mode' => 'value'], // utm_source
    1050089 => ['target_field_id' => 1138343, 'mode' => 'value'], // utm_term
    1050091 => ['target_field_id' => 1138345, 'mode' => 'value'], // utm_referrer
    1050093 => ['target_field_id' => 1138347, 'mode' => 'value'], // roistat
    1050095 => ['target_field_id' => 1138349, 'mode' => 'value'], // referrer
    1050097 => ['target_field_id' => 1138351, 'mode' => 'value'], // openstat_service
    1050099 => ['target_field_id' => 1138353, 'mode' => 'value'], // openstat_campaign
    1050101 => ['target_field_id' => 1138355, 'mode' => 'value'], // openstat_ad
    1050103 => ['target_field_id' => 1138357, 'mode' => 'value'], // openstat_source
    1050105 => ['target_field_id' => 1138359, 'mode' => 'value'], // from
    1050107 => ['target_field_id' => 1138361, 'mode' => 'value'], // gclientid
    1050109 => ['target_field_id' => 1138363, 'mode' => 'value'], // _ym_uid
    1050111 => ['target_field_id' => 1138365, 'mode' => 'value'], // _ym_counter
    1050113 => ['target_field_id' => 1138367, 'mode' => 'value'], // gclid
    1050115 => ['target_field_id' => 1138369, 'mode' => 'value'], // yclid
    1050117 => ['target_field_id' => 1138371, 'mode' => 'value'], // fbclid
    // 0 => ['target_field_id' => 0, 'mode' => 'value'],
    // Пример для списочного поля с разными enum_id:
    // 123456 => [
    //     'target_field_id' => 654321,
    //     'mode' => 'enum_id',
    //     'enum_map' => [111 => 222, 333 => 444],
    // ],
];

// Маппинг полей контакта: source_field_id => настройки переноса в target.
const TARGET_CONTACT_FIELD_MAP = [
    1050071 => ['target_field_id' => 1138325, 'mode' => 'value'], // Должность
    1073649 => ['target_field_id' => 1575287, 'mode' => 'value'], // телефон плательщика -> Дополнительный телефон
    1333205 => ['target_field_id' => 1621101, 'mode' => 'value'], // Telegram ID
    1333207 => ['target_field_id' => 1629273, 'mode' => 'value'], // Telegram username
    1473995 => ['target_field_id' => 1630424, 'mode' => 'value'], // Max ID
    1528299 => ['target_field_id' => 1631985, 'mode' => 'value'], // Max User ID
];

// Не найдены соответствующие поля на target-портале (добавить после создания полей):
// - Статус ученика
// - Вид занятий
// - Курс изучения
// - Имя плательщика
// - Компания
// - раб тел
// - емаил раб

cp_log('INFO', 'Получен вебхук кросс-портального переноса', ['post' => $_POST]);

try {
    $payload = cp_get_input_payload();
    $leadId = cp_extract_lead_id($payload);

    if ($leadId <= 0) {
        throw new RuntimeException('В вебхуке отсутствует корректный lead_id');
    }

    $sourceLead = cp_fetch_source_lead($leadId);
    $classification = cp_classify_source_lead($sourceLead);

    if ($classification === null) {
        cp_log('INFO', 'Сделка не в статусе закрыто реализовано/не реализовано, пропуск', [
            'lead_id' => $leadId,
            'source_status_id' => $sourceLead['status_id'] ?? null,
            'source_pipeline_id' => $sourceLead['pipeline_id'] ?? null,
        ]);

        http_response_code(200);
        echo 'OK - skipped';
        exit;
    }

    $targetLeadId = cp_create_target_lead($sourceLead, $classification);

    cp_log('INFO', 'Сделка создана на целевом портале', [
        'source_lead_id' => $leadId,
        'target_lead_id' => $targetLeadId,
        'classification' => $classification,
    ]);

    header('Content-Type: application/json; charset=utf-8');
    http_response_code(200);
    echo json_encode([
        'success' => true,
        'source_lead_id' => $leadId,
        'target_lead_id' => $targetLeadId,
        'classification' => $classification,
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
} catch (Throwable $e) {
    cp_log('ERROR', 'Ошибка кросс-портального обработчика', [
        'error' => $e->getMessage(),
        'trace' => $e->getTraceAsString(),
    ]);

    header('Content-Type: application/json; charset=utf-8');
    http_response_code(500);
    echo json_encode([
        'success' => false,
        'error' => $e->getMessage(),
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
}

function cp_get_input_payload(): array
{
    if (!empty($_POST)) {
        return $_POST;
    }

    $raw = file_get_contents('php://input');
    if ($raw === false || trim($raw) === '') {
        return [];
    }

    $decoded = json_decode($raw, true);
    return is_array($decoded) ? $decoded : [];
}

function cp_extract_lead_id(array $payload): int
{
    $directLeadId = (int) ($payload['lead_id'] ?? 0);
    if ($directLeadId > 0) {
        return $directLeadId;
    }

    $leadIdFromStatus = (int) ($payload['leads']['status'][0]['id'] ?? 0);
    if ($leadIdFromStatus > 0) {
        return $leadIdFromStatus;
    }

    $leadIdFromUpdate = (int) ($payload['leads']['update'][0]['id'] ?? 0);
    if ($leadIdFromUpdate > 0) {
        return $leadIdFromUpdate;
    }

    $leadIdFromAdd = (int) ($payload['leads']['add'][0]['id'] ?? 0);
    if ($leadIdFromAdd > 0) {
        return $leadIdFromAdd;
    }

    return 0;
}

function cp_log(string $level, string $message, array $context = []): void
{
    $logDir = dirname(HANDLER_LOG_FILE);
    if (!is_dir($logDir)) {
        @mkdir($logDir, 0755, true);
    }

    $line = '[' . date('Y-m-d H:i:s') . ']';
    $line .= ' [' . strtoupper($level) . ']';
    $line .= ' [' . HANDLER_SOURCE . '] ' . $message;

    if (!empty($context)) {
        $line .= "\n" . json_encode($context, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
    }

    $line .= "\n" . str_repeat('-', 80) . "\n";
    @file_put_contents(HANDLER_LOG_FILE, $line, FILE_APPEND | LOCK_EX);
}

function cp_fetch_source_lead(int $leadId): array
{
    $response = cp_source_request("/api/v4/leads/{$leadId}?with=contacts");

    if (!isset($response['id'])) {
        throw new RuntimeException('Источник не вернул данные сделки');
    }

    return $response;
}

function cp_classify_source_lead(array $lead): ?string
{
    $pipelineId = (int) ($lead['pipeline_id'] ?? 0);
    $statusId = (int) ($lead['status_id'] ?? 0);

    if ($pipelineId <= 0 || $statusId <= 0) {
        return null;
    }

    $pipeline = cp_source_request("/api/v4/leads/pipelines/{$pipelineId}");
    $statuses = $pipeline['_embedded']['statuses'] ?? [];

    $statusName = '';
    foreach ($statuses as $status) {
        if ((int) ($status['id'] ?? 0) === $statusId) {
            $statusName = (string) ($status['name'] ?? '');
            break;
        }
    }

    if ($statusName === '') {
        return null;
    }

    $normalized = mb_strtolower(trim((string) preg_replace('/\s+/u', ' ', $statusName)), 'UTF-8');

    if (str_contains($normalized, 'не реализ')) {
        return 'lost';
    }

    if (
        str_contains($normalized, 'закрыто реализ') ||
        str_contains($normalized, 'успешно реализ') ||
        str_contains($normalized, 'реализовано')
    ) {
        return 'won';
    }

    return null;
}

function cp_create_target_lead(array $sourceLead, string $classification): int
{
    global $subdomain, $data;

    if (TARGET_AMO_PIPELINE_ID <= 0 || TARGET_AMO_STATUS_ID <= 0) {
        throw new RuntimeException('Не заданы TARGET_AMO_PIPELINE_ID и/или TARGET_AMO_STATUS_ID');
    }

    $sourceLeadId = (int) ($sourceLead['id'] ?? 0);
    $sourceLeadName = trim((string) ($sourceLead['name'] ?? 'Без названия'));
    $mappedCustomFields = cp_build_target_custom_fields($sourceLead);
    $sourceContact = cp_extract_primary_source_contact($sourceLead);
    $targetLeadName = $sourceLeadName . ' [source #' . $sourceLeadId . ']';

    $existingLeadId = cp_find_existing_target_lead_id($sourceLeadId, $targetLeadName);
    if ($existingLeadId > 0) {
        cp_log('INFO', 'Сделка уже существует на целевом портале, повторное создание пропущено', [
            'source_lead_id' => $sourceLeadId,
            'target_lead_id' => $existingLeadId,
        ]);

        return $existingLeadId;
    }

    $targetLead = [
        'name' => $targetLeadName,
        'price' => (int) ($sourceLead['price'] ?? 0),
        'pipeline_id' => TARGET_AMO_PIPELINE_ID,
        'status_id' => TARGET_AMO_STATUS_ID,
    ];

    if (!empty($mappedCustomFields)) {
        $targetLead['custom_fields_values'] = $mappedCustomFields;
    }

    $targetPayload = [$targetLead];

    cp_log('INFO', 'Сформированы поля для переноса', [
        'source_lead_id' => $sourceLeadId,
        'mapped_fields_count' => count($mappedCustomFields),
        'contact_transferred' => !empty($sourceContact),
    ]);

    $response = cp_target_request('POST', '/api/v4/leads', $targetPayload);
    $createdLeadId = (int) ($response['_embedded']['leads'][0]['id'] ?? 0);

    if ($createdLeadId <= 0) {
        throw new RuntimeException('Целевой портал не вернул ID созданной сделки');
    }

    if (!empty($sourceContact)) {
        $targetContactId = cp_create_target_contact($sourceContact);
        cp_link_contact_to_target_lead($createdLeadId, $targetContactId);

        cp_log('INFO', 'Контакт создан и привязан к сделке на целевом портале', [
            'source_lead_id' => $sourceLeadId,
            'target_lead_id' => $createdLeadId,
            'target_contact_id' => $targetContactId,
        ]);
    }

    return $createdLeadId;
}

function cp_extract_primary_source_contact(array $sourceLead): array
{
    $contactId = (int) ($sourceLead['_embedded']['contacts'][0]['id'] ?? 0);
    if ($contactId <= 0) {
        return [];
    }

    $sourceContact = cp_source_request('/api/v4/contacts/' . $contactId);
    if (!is_array($sourceContact) || empty($sourceContact['id'])) {
        return [];
    }

    $phones = [];
    $emails = [];

    foreach (($sourceContact['custom_fields_values'] ?? []) as $field) {
        $fieldCode = (string) ($field['field_code'] ?? '');
        $values = $field['values'] ?? [];

        if ($fieldCode === 'PHONE') {
            foreach ($values as $value) {
                if (!empty($value['value'])) {
                    $phones[] = (string) $value['value'];
                }
            }
        }

        if ($fieldCode === 'EMAIL') {
            foreach ($values as $value) {
                if (!empty($value['value'])) {
                    $emails[] = (string) $value['value'];
                }
            }
        }
    }

    return [
        'name' => trim((string) ($sourceContact['name'] ?? '')),
        'phones' => array_values(array_unique($phones)),
        'emails' => array_values(array_unique($emails)),
        'custom_fields_values' => is_array($sourceContact['custom_fields_values'] ?? null) ? $sourceContact['custom_fields_values'] : [],
    ];
}

function cp_build_target_contact_payload(array $sourceContact): array
{
    $contact = [
        'name' => $sourceContact['name'] !== '' ? $sourceContact['name'] : 'Без имени',
    ];

    $contactFields = [];

    if (!empty($sourceContact['phones'])) {
        $contactFields[] = [
            'field_code' => 'PHONE',
            'values' => array_map(static function (string $phone): array {
                return ['value' => $phone, 'enum_code' => 'WORK'];
            }, $sourceContact['phones']),
        ];
    }

    if (!empty($sourceContact['emails'])) {
        $contactFields[] = [
            'field_code' => 'EMAIL',
            'values' => array_map(static function (string $email): array {
                return ['value' => $email, 'enum_code' => 'WORK'];
            }, $sourceContact['emails']),
        ];
    }

    $mappedContactFields = cp_build_mapped_fields(
        $sourceContact['custom_fields_values'] ?? [],
        TARGET_CONTACT_FIELD_MAP,
        'contacts'
    );

    if (!empty($mappedContactFields)) {
        foreach ($mappedContactFields as $mappedField) {
            $contactFields[] = $mappedField;
        }
    }

    if (!empty($contactFields)) {
        $contact['custom_fields_values'] = $contactFields;
    }

    return $contact;
}

function cp_create_target_contact(array $sourceContact): int
{
    $payload = [cp_build_target_contact_payload($sourceContact)];
    $response = cp_target_request('POST', '/api/v4/contacts', $payload);
    $contactId = (int) ($response['_embedded']['contacts'][0]['id'] ?? 0);

    if ($contactId <= 0) {
        throw new RuntimeException('Целевой портал не вернул ID созданного контакта');
    }

    return $contactId;
}

function cp_link_contact_to_target_lead(int $leadId, int $contactId): void
{
    if ($leadId <= 0 || $contactId <= 0) {
        throw new RuntimeException('Некорректные ID сделки или контакта для привязки');
    }

    cp_target_request('POST', '/api/v4/leads/' . $leadId . '/link', [
        [
            'to_entity_id' => $contactId,
            'to_entity_type' => 'contacts',
        ],
    ]);
}

function cp_find_existing_target_lead_id(int $sourceLeadId, string $expectedName): int
{
    if ($sourceLeadId <= 0) {
        return 0;
    }

    $marker = '[source #' . $sourceLeadId . ']';
    $response = cp_target_request('GET', '/api/v4/leads?query=' . urlencode($marker) . '&limit=50');
    $leads = $response['_embedded']['leads'] ?? [];

    if (!is_array($leads)) {
        return 0;
    }

    foreach ($leads as $lead) {
        $leadId = (int) ($lead['id'] ?? 0);
        $leadName = trim((string) ($lead['name'] ?? ''));
        $pipelineId = (int) ($lead['pipeline_id'] ?? 0);

        if ($leadId > 0 && $pipelineId === TARGET_AMO_PIPELINE_ID && $leadName === $expectedName) {
            return $leadId;
        }
    }

    return 0;
}

function cp_build_target_custom_fields(array $sourceLead): array
{
    return cp_build_mapped_fields($sourceLead['custom_fields_values'] ?? [], TARGET_LEAD_FIELD_MAP, 'leads');
}

function cp_build_mapped_fields(array $sourceFields, array $fieldMap, string $entityType): array
{
    if (!is_array($sourceFields) || empty($sourceFields)) {
        return [];
    }

    $targetFieldMetaById = cp_get_target_field_meta_by_id($entityType);

    $result = [];

    foreach ($sourceFields as $sourceField) {
        $sourceFieldId = (int) ($sourceField['field_id'] ?? 0);
        if ($sourceFieldId <= 0 || !isset($fieldMap[$sourceFieldId])) {
            continue;
        }

        $map = $fieldMap[$sourceFieldId];
        $targetFieldId = (int) ($map['target_field_id'] ?? 0);
        if ($targetFieldId <= 0) {
            continue;
        }

        if (!cp_is_target_field_available_for_pipeline($targetFieldMetaById, $targetFieldId, TARGET_AMO_PIPELINE_ID)) {
            continue;
        }

        $sourceValues = $sourceField['values'] ?? [];
        if (!is_array($sourceValues) || empty($sourceValues)) {
            continue;
        }

        $mode = (string) ($map['mode'] ?? 'value');
        $mappedValues = [];

        foreach ($sourceValues as $sourceValue) {
            if (!is_array($sourceValue)) {
                continue;
            }

            if ($mode === 'enum_by_value') {
                $sourceText = trim((string) ($sourceValue['value'] ?? ''));
                if ($sourceText === '') {
                    continue;
                }

                $targetEnumId = cp_find_target_enum_id_by_value(
                    $targetFieldMetaById,
                    $targetFieldId,
                    $sourceText
                );

                if ($targetEnumId > 0) {
                    $mappedValues[] = ['enum_id' => $targetEnumId];
                }
                continue;
            }

            if ($mode === 'enum_id') {
                $sourceEnumId = (int) ($sourceValue['enum_id'] ?? 0);
                $enumMap = $map['enum_map'] ?? [];
                $targetEnumId = (int) ($enumMap[$sourceEnumId] ?? 0);
                if ($targetEnumId > 0) {
                    $mappedValues[] = ['enum_id' => $targetEnumId];
                }
                continue;
            }

            if ($mode === 'enum_text_map') {
                $sourceText = cp_normalize_name((string) ($sourceValue['value'] ?? ''));
                $textMap = is_array($map['text_map'] ?? null) ? $map['text_map'] : [];
                $targetText = (string) ($textMap[$sourceText] ?? '');
                if ($targetText === '') {
                    continue;
                }

                $targetEnumId = cp_find_target_enum_id_by_value(
                    $targetFieldMetaById,
                    $targetFieldId,
                    $targetText
                );

                if ($targetEnumId > 0) {
                    $mappedValues[] = ['enum_id' => $targetEnumId];
                }
                continue;
            }

            if (array_key_exists('value', $sourceValue)) {
                $mappedValues[] = ['value' => $sourceValue['value']];
            }
        }

        $mappedValues = cp_normalize_target_field_values($targetFieldMetaById, $targetFieldId, $mappedValues);

        if (!empty($mappedValues)) {
            $result[] = [
                'field_id' => $targetFieldId,
                'values' => $mappedValues,
            ];
        }
    }

    return $result;
}

function cp_normalize_target_field_values(array $targetFieldMetaById, int $targetFieldId, array $mappedValues): array
{
    if ($mappedValues === []) {
        return [];
    }

    $targetField = $targetFieldMetaById[$targetFieldId] ?? null;
    if (!is_array($targetField)) {
        return $mappedValues;
    }

    $fieldType = (string) ($targetField['type'] ?? '');

    if (in_array($fieldType, ['select', 'radiobutton'], true)) {
        return [reset($mappedValues)];
    }

    if ($fieldType === 'checkbox') {
        return [end($mappedValues)];
    }

    if (in_array($fieldType, ['text', 'numeric', 'date', 'date_time', 'url'], true)) {
        return [reset($mappedValues)];
    }

    return $mappedValues;
}

function cp_is_target_field_available_for_pipeline(array $targetFieldMetaById, int $targetFieldId, int $pipelineId): bool
{
    $targetField = $targetFieldMetaById[$targetFieldId] ?? null;
    if (!is_array($targetField)) {
        return false;
    }

    $requiredStatuses = $targetField['required_statuses'] ?? [];
    if (!is_array($requiredStatuses) || $requiredStatuses === []) {
        return true;
    }

    foreach ($requiredStatuses as $requiredStatus) {
        if ((int) ($requiredStatus['pipeline_id'] ?? 0) === $pipelineId) {
            return true;
        }
    }

    return false;
}

function cp_get_target_field_meta_by_id(string $entityType): array
{
    static $cache = [];

    if (isset($cache[$entityType])) {
        return $cache[$entityType];
    }

    $endpoint = $entityType === 'contacts' ? '/api/v4/contacts/custom_fields' : '/api/v4/leads/custom_fields';
    $response = cp_target_request('GET', $endpoint);
    $fields = $response['_embedded']['custom_fields'] ?? [];

    $byId = [];
    foreach ($fields as $field) {
        $id = (int) ($field['id'] ?? 0);
        if ($id > 0) {
            $byId[$id] = $field;
        }
    }

    $cache[$entityType] = $byId;
    return $byId;
}

function cp_find_target_enum_id_by_value(array $targetFieldMetaById, int $targetFieldId, string $sourceValue): int
{
    $targetField = $targetFieldMetaById[$targetFieldId] ?? null;
    if (!is_array($targetField)) {
        return 0;
    }

    $enums = $targetField['enums'] ?? [];
    if (!is_array($enums) || empty($enums)) {
        return 0;
    }

    $normalizedSource = cp_normalize_name($sourceValue);

    foreach ($enums as $enumId => $enumValue) {
        if (is_array($enumValue)) {
            $value = (string) ($enumValue['value'] ?? '');
            $id = (int) ($enumValue['id'] ?? 0);
            if ($id > 0 && $value !== '' && cp_normalize_name($value) === $normalizedSource) {
                return $id;
            }
            continue;
        }

        if (cp_normalize_name((string) $enumValue) === $normalizedSource) {
            return (int) $enumId;
        }
    }

    return 0;
}

function cp_normalize_name(string $value): string
{
    $value = mb_strtolower(trim($value), 'UTF-8');
    $value = str_replace('ё', 'е', $value);
    $value = (string) preg_replace('/\s+/u', ' ', $value);
    return $value;
}

function cp_source_request(string $path): array
{
    $url = rtrim(SOURCE_AMO_BASE_URL, '/') . $path;

    $headers = [
        'Authorization: Bearer ' . SOURCE_AMO_ACCESS_TOKEN,
        'Content-Type: application/json',
    ];

    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => $url,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_HTTPHEADER => $headers,
        CURLOPT_USERAGENT => 'amoCRM-oAuth-client/1.0',
        CURLOPT_SSL_VERIFYPEER => 1,
        CURLOPT_SSL_VERIFYHOST => 2,
        CURLOPT_TIMEOUT => 30,
        CURLOPT_CONNECTTIMEOUT => 10,
    ]);

    $raw = curl_exec($ch);
    $httpCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlError = curl_error($ch);
    curl_close($ch);

    if ($curlError !== '') {
        throw new RuntimeException('Ошибка запроса к источнику: ' . $curlError);
    }

    if ($httpCode < 200 || $httpCode > 204) {
        throw new RuntimeException('Источник вернул HTTP ' . $httpCode . ': ' . (string) $raw);
    }

    $decoded = json_decode((string) $raw, true);
    if (!is_array($decoded)) {
        throw new RuntimeException('Источник вернул некорректный JSON');
    }

    return $decoded;
}

function cp_target_request(string $method, string $path, ?array $payload = null): array
{
    global $subdomain, $data;

    $url = 'https://' . $subdomain . '.amocrm.ru' . $path;
    $headers = [
        'Authorization: Bearer ' . ($data['access_token'] ?? ''),
        'Content-Type: application/json',
    ];

    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => $url,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_HTTPHEADER => $headers,
        CURLOPT_USERAGENT => 'amoCRM-oAuth-client/1.0',
        CURLOPT_SSL_VERIFYPEER => 1,
        CURLOPT_SSL_VERIFYHOST => 2,
        CURLOPT_TIMEOUT => 30,
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_CUSTOMREQUEST => strtoupper($method),
    ]);

    if ($payload !== null) {
        curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
    }

    $raw = curl_exec($ch);
    $httpCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlError = curl_error($ch);
    curl_close($ch);

    if ($curlError !== '') {
        throw new RuntimeException('Ошибка запроса к целевому порталу: ' . $curlError);
    }

    if ($httpCode < 200 || $httpCode > 204) {
        cp_log('ERROR', 'Целевой портал вернул ошибку', [
            'method' => strtoupper($method),
            'path' => $path,
            'http_code' => $httpCode,
            'payload' => $payload,
            'response' => (string) $raw,
        ]);

        throw new RuntimeException('Целевой портал вернул HTTP ' . $httpCode . ': ' . (string) $raw);
    }

    if ($raw === '' || $raw === null) {
        return [];
    }

    $decoded = json_decode((string) $raw, true);
    if (!is_array($decoded)) {
        throw new RuntimeException('Целевой портал вернул некорректный JSON');
    }

    return $decoded;
}
