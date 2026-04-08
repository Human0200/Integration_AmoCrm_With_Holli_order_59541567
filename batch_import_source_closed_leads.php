<?php

declare(strict_types=1);

require_once __DIR__ . '/logger.php';
require_once __DIR__ . '/config.php';
require_once __DIR__ . '/amo_func.php';

const BATCH_IMPORT_SOURCE = 'batch_import_source_closed_leads.php';
const BATCH_IMPORT_LOG_FILE = __DIR__ . '/logs/batch_import_source_closed_leads.log';
const SOURCE_AMO_ACCESS_TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6ImM2MDc4MDY1ZTczN2IwOWU1MTcxMDY5ZjUzZmIyN2QwNzBjMTA3YzIyODI0NzAxNzAzNjdhZGI2NzlhM2MwOWEyNzM1NGI5ZDk5ZjE1ZDMzIn0.eyJhdWQiOiIwNTQ0ZDY3NC00MmI4LTRiYTMtOTBlZC02OTM4MGFkMzNjNWQiLCJqdGkiOiJjNjA3ODA2NWU3MzdiMDllNTE3MTA2OWY1M2ZiMjdkMDcwYzEwN2MyMjgyNDcwMTcwMzY3YWRiNjc5YTNjMDlhMjczNTRiOWQ5OWYxNWQzMyIsImlhdCI6MTc3NTY2NDY0NSwibmJmIjoxNzc1NjY0NjQ1LCJleHAiOjE3ODAxODU2MDAsInN1YiI6IjEyNjQyMzE0IiwiZ3JhbnRfdHlwZSI6IiIsImFjY291bnRfaWQiOjMyNDk1NjI2LCJiYXNlX2RvbWFpbiI6ImFtb2NybS5ydSIsInZlcnNpb24iOjIsInNjb3BlcyI6WyJwdXNoX25vdGlmaWNhdGlvbnMiLCJmaWxlcyIsImNybSIsImZpbGVzX2RlbGV0ZSIsIm5vdGlmaWNhdGlvbnMiXSwiaGFzaF91dWlkIjoiMGFiYmNhYWUtYzUzNi00NzMwLWEyODAtNDdjNjAwMjBkMGJmIiwiYXBpX2RvbWFpbiI6ImFwaS1iLmFtb2NybS5ydSJ9.GWXm7Z4v0eXQcb9w3SrlTdeY31GGzJf4Ta7EZZune6O61j520bu-rQKyLeUhJNjZz4tPDLBbf7l5_P-60QGAg_aW-JwCwExpMb04_0FLZIl4GcclB_dnT9zoccvsrXe58bNl50du4hiAhan58GWb51K9zM0BI8A0cOB9Hasytno1dJ_eLF9euyYlP5d_yRqV-5TsryUCg6PDwQaKgfZsjOh-bHY60vog6NHuM5u66BN6l_4XI44tTARWYDySRx9UpwnsQLkCgDUfPKHE9ij4D92-hJJSkUubn9jygblVasxoOz0oCH65Rx38Jo2wHuZ21RjgGXOp-Dwb5gBOW3zoxA';
const SOURCE_AMO_BASE_URL = 'https://supportchinatutorru.amocrm.ru';
const SOURCE_STATUSES_FILE = __DIR__ . '/source_closed_statuses.json';
const IMPORT_STATE_FILE = __DIR__ . '/batch_import_source_closed_leads_state.json';

const TARGET_AMO_PIPELINE_ID = 9919562;
const TARGET_AMO_STATUS_ID = 84476154;

const TARGET_LEAD_FIELD_MAP = [
    1054565 => ['target_field_id' => 1604231, 'mode' => 'value'],
    1054615 => ['target_field_id' => 1575217, 'mode' => 'enum_by_value'],
    1054623 => [
        'target_field_id' => 1618692,
        'mode' => 'enum_text_map',
        'text_map' => [
            'действующий' => 'да',
            'не действующий' => 'нет',
            'недействующий' => 'нет',
            'новый' => 'нет',
        ],
    ],
    1054625 => ['target_field_id' => 1575317, 'mode' => 'enum_by_value'],
    1649627 => ['target_field_id' => 1598451, 'mode' => 'value'],
    1698261 => ['target_field_id' => 1576357, 'mode' => 'enum_by_value'],
    1060101 => ['target_field_id' => 1630807, 'mode' => 'value'],
    1050081 => ['target_field_id' => 1138335, 'mode' => 'value'],
    1050083 => ['target_field_id' => 1138337, 'mode' => 'value'],
    1050085 => ['target_field_id' => 1138339, 'mode' => 'value'],
    1050087 => ['target_field_id' => 1138341, 'mode' => 'value'],
    1050089 => ['target_field_id' => 1138343, 'mode' => 'value'],
    1050091 => ['target_field_id' => 1138345, 'mode' => 'value'],
    1050093 => ['target_field_id' => 1138347, 'mode' => 'value'],
    1050095 => ['target_field_id' => 1138349, 'mode' => 'value'],
    1050097 => ['target_field_id' => 1138351, 'mode' => 'value'],
    1050099 => ['target_field_id' => 1138353, 'mode' => 'value'],
    1050101 => ['target_field_id' => 1138355, 'mode' => 'value'],
    1050103 => ['target_field_id' => 1138357, 'mode' => 'value'],
    1050105 => ['target_field_id' => 1138359, 'mode' => 'value'],
    1050107 => ['target_field_id' => 1138361, 'mode' => 'value'],
    1050109 => ['target_field_id' => 1138363, 'mode' => 'value'],
    1050111 => ['target_field_id' => 1138365, 'mode' => 'value'],
    1050113 => ['target_field_id' => 1138367, 'mode' => 'value'],
    1050115 => ['target_field_id' => 1138369, 'mode' => 'value'],
    1050117 => ['target_field_id' => 1138371, 'mode' => 'value'],
];

const TARGET_CONTACT_FIELD_MAP = [
    1050071 => ['target_field_id' => 1138325, 'mode' => 'value'],
    1073649 => ['target_field_id' => 1575287, 'mode' => 'value'],
    1333205 => ['target_field_id' => 1621101, 'mode' => 'value'],
    1333207 => ['target_field_id' => 1629273, 'mode' => 'value'],
    1473995 => ['target_field_id' => 1630424, 'mode' => 'value'],
    1528299 => ['target_field_id' => 1631985, 'mode' => 'value'],
];

bi_log('INFO', 'Старт пакетного импорта закрытых лидов', [
    'get' => $_GET,
    'cli' => PHP_SAPI === 'cli' ? ($_SERVER['argv'] ?? []) : null,
]);

try {
    $options = bi_get_options();
    $statusMap = bi_load_source_status_map();
    $state = bi_load_state();
    $leadIds = $options['lead_id'] > 0
        ? [$options['lead_id']]
        : bi_fetch_source_lead_ids($statusMap, $options['limit']);

    $stats = [
        'found' => count($leadIds),
        'processed' => 0,
        'created' => 0,
        'skipped_state' => 0,
        'skipped_existing' => 0,
        'dry_run' => 0,
        'errors' => 0,
    ];
    $results = [];

    foreach ($leadIds as $leadId) {
        $leadId = (int) $leadId;
        if ($leadId <= 0) {
            continue;
        }

        if (!$options['force'] && isset($state['processed'][$leadId])) {
            $stats['skipped_state']++;
            $results[] = [
                'source_lead_id' => $leadId,
                'status' => 'skipped_state',
                'note' => 'Уже был обработан этим batch-скриптом',
            ];
            continue;
        }

        try {
            $sourceLead = bi_fetch_source_lead($leadId);
            $classification = bi_classify_source_lead($sourceLead, $statusMap);

            if ($classification === null) {
                $results[] = [
                    'source_lead_id' => $leadId,
                    'status' => 'skipped_unknown_status',
                ];
                continue;
            }

            $targetLeadName = bi_build_target_lead_name($sourceLead);
            $existingTargetLeadId = bi_find_existing_target_lead_id($targetLeadName);

            if ($existingTargetLeadId > 0) {
                if ($options['force']) {
                    $updatedLeadId = bi_update_target_lead($existingTargetLeadId, $sourceLead, $classification);
                    $stats['processed']++;
                    $state['processed'][$leadId] = [
                        'status' => 'updated_existing',
                        'target_lead_id' => $updatedLeadId,
                        'processed_at' => date(DATE_ATOM),
                    ];
                    $results[] = [
                        'source_lead_id' => $leadId,
                        'target_lead_id' => $updatedLeadId,
                        'classification' => $classification,
                        'status' => 'updated_existing',
                    ];
                    continue;
                }

                $stats['skipped_existing']++;
                $state['processed'][$leadId] = [
                    'status' => 'skipped_existing',
                    'target_lead_id' => $existingTargetLeadId,
                    'processed_at' => date(DATE_ATOM),
                ];
                $results[] = [
                    'source_lead_id' => $leadId,
                    'target_lead_id' => $existingTargetLeadId,
                    'classification' => $classification,
                    'status' => 'skipped_existing',
                ];
                continue;
            }

            if ($options['dry_run']) {
                $stats['dry_run']++;
                $results[] = [
                    'source_lead_id' => $leadId,
                    'classification' => $classification,
                    'status' => 'dry_run',
                    'target_lead_name' => $targetLeadName,
                ];
                continue;
            }

            $targetLeadId = bi_create_target_lead($sourceLead, $classification);
            $stats['created']++;
            $stats['processed']++;
            $state['processed'][$leadId] = [
                'status' => 'created',
                'target_lead_id' => $targetLeadId,
                'processed_at' => date(DATE_ATOM),
            ];
            $results[] = [
                'source_lead_id' => $leadId,
                'target_lead_id' => $targetLeadId,
                'classification' => $classification,
                'status' => 'created',
            ];
        } catch (Throwable $e) {
            $stats['errors']++;
            $state['processed'][$leadId] = [
                'status' => 'error',
                'error' => $e->getMessage(),
                'processed_at' => date(DATE_ATOM),
            ];
            $results[] = [
                'source_lead_id' => $leadId,
                'status' => 'error',
                'error' => $e->getMessage(),
            ];
            bi_log('ERROR', 'Ошибка обработки лида', [
                'source_lead_id' => $leadId,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
        }

        bi_save_state($state);
    }

    bi_log('INFO', 'Пакетный импорт завершен', [
        'options' => $options,
        'stats' => $stats,
    ]);

    bi_respond([
        'success' => true,
        'options' => $options,
        'stats' => $stats,
        'results' => $results,
    ], 200);
} catch (Throwable $e) {
    bi_log('ERROR', 'Критическая ошибка пакетного импорта', [
        'error' => $e->getMessage(),
        'trace' => $e->getTraceAsString(),
    ]);

    bi_respond([
        'success' => false,
        'error' => $e->getMessage(),
    ], 500);
}

function bi_get_options(): array
{
    $input = PHP_SAPI === 'cli' ? bi_parse_cli_args($_SERVER['argv'] ?? []) : $_GET;

    return [
        'dry_run' => bi_to_bool($input['dry_run'] ?? false),
        'force' => bi_to_bool($input['force'] ?? false),
        'limit' => max(0, (int) ($input['limit'] ?? 0)),
        'lead_id' => max(0, (int) ($input['lead_id'] ?? 0)),
    ];
}

function bi_parse_cli_args(array $argv): array
{
    $result = [];
    foreach ($argv as $index => $arg) {
        if ($index === 0 || !str_starts_with($arg, '--')) {
            continue;
        }

        $pair = explode('=', substr($arg, 2), 2);
        $result[$pair[0]] = $pair[1] ?? '1';
    }

    return $result;
}

function bi_to_bool(mixed $value): bool
{
    if (is_bool($value)) {
        return $value;
    }

    $normalized = mb_strtolower(trim((string) $value), 'UTF-8');
    return in_array($normalized, ['1', 'true', 'yes', 'y', 'on'], true);
}

function bi_load_source_status_map(): array
{
    if (!is_file(SOURCE_STATUSES_FILE)) {
        throw new RuntimeException('Не найден файл со статусами: ' . SOURCE_STATUSES_FILE);
    }

    $raw = file_get_contents(SOURCE_STATUSES_FILE);
    $decoded = json_decode((string) $raw, true);
    $statuses = $decoded['statuses'] ?? null;

    if (!is_array($statuses) || $statuses === []) {
        throw new RuntimeException('Файл source_closed_statuses.json не содержит статусов');
    }

    $map = [];
    foreach ($statuses as $item) {
        $pipelineId = (int) ($item['pipeline_id'] ?? 0);
        $statusId = (int) ($item['status_id'] ?? 0);
        $classification = (string) ($item['classification'] ?? '');

        if ($pipelineId <= 0 || $statusId <= 0 || $classification === '') {
            continue;
        }

        $map[$pipelineId . ':' . $statusId] = [
            'pipeline_id' => $pipelineId,
            'status_id' => $statusId,
            'classification' => $classification,
            'pipeline_name' => (string) ($item['pipeline_name'] ?? ''),
            'status_name' => (string) ($item['status_name'] ?? ''),
        ];
    }

    if ($map === []) {
        throw new RuntimeException('Не удалось построить карту статусов из source_closed_statuses.json');
    }

    return $map;
}

function bi_fetch_source_lead_ids(array $statusMap, int $limit = 0): array
{
    $page = 1;
    $leadIds = [];
    $pageSize = 250;

    do {
        $query = ['page' => $page, 'limit' => $pageSize];
        $i = 0;

        foreach ($statusMap as $status) {
            $query["filter[statuses][{$i}][pipeline_id]"] = $status['pipeline_id'];
            $query["filter[statuses][{$i}][status_id]"] = $status['status_id'];
            $i++;
        }

        $response = bi_source_request('/api/v4/leads?' . http_build_query($query));
        $pageItems = $response['_embedded']['leads'] ?? [];

        if (!is_array($pageItems) || $pageItems === []) {
            break;
        }

        foreach ($pageItems as $lead) {
            $leadId = (int) ($lead['id'] ?? 0);
            $createdAt = (int) ($lead['created_at'] ?? 0);
            if ($leadId > 0) {
                $leadIds[$leadId] = [
                    'id' => $leadId,
                    'created_at' => $createdAt,
                ];
            }
        }

        $page++;
    } while (count($pageItems) === $pageSize);

    if ($leadIds === []) {
        return [];
    }

    uasort($leadIds, static function (array $left, array $right): int {
        $createdAtCompare = ($right['created_at'] ?? 0) <=> ($left['created_at'] ?? 0);
        if ($createdAtCompare !== 0) {
            return $createdAtCompare;
        }

        return ($right['id'] ?? 0) <=> ($left['id'] ?? 0);
    });

    $sortedIds = array_map(
        static fn(array $item): int => (int) $item['id'],
        array_values($leadIds)
    );

    if ($limit > 0) {
        return array_slice($sortedIds, 0, $limit);
    }

    return $sortedIds;
}

function bi_fetch_source_lead(int $leadId): array
{
    $response = bi_source_request("/api/v4/leads/{$leadId}?with=contacts");

    if (!isset($response['id'])) {
        throw new RuntimeException('Источник не вернул данные сделки #' . $leadId);
    }

    return $response;
}

function bi_classify_source_lead(array $lead, array $statusMap): ?string
{
    $pipelineId = (int) ($lead['pipeline_id'] ?? 0);
    $statusId = (int) ($lead['status_id'] ?? 0);
    $key = $pipelineId . ':' . $statusId;

    return isset($statusMap[$key]) ? (string) $statusMap[$key]['classification'] : null;
}

function bi_build_target_lead_name(array $sourceLead): string
{
    $sourceLeadId = (int) ($sourceLead['id'] ?? 0);
    $sourceLeadName = trim((string) ($sourceLead['name'] ?? 'Без названия'));
    return $sourceLeadName . ' [source #' . $sourceLeadId . ']';
}

function bi_find_existing_target_lead_id(string $leadName): int
{
    global $subdomain, $data;

    $url = '/api/v4/leads?' . http_build_query([
        'filter[name]' => $leadName,
        'limit' => 1,
    ]);
    $response = get($subdomain, $url, $data);
    $leads = $response['_embedded']['leads'] ?? [];

    foreach ($leads as $lead) {
        if ((string) ($lead['name'] ?? '') === $leadName) {
            return (int) ($lead['id'] ?? 0);
        }
    }

    return 0;
}

function bi_create_target_lead(array $sourceLead, string $classification): int
{
    global $subdomain, $data;

    $sourceLeadId = (int) ($sourceLead['id'] ?? 0);
    $mappedCustomFields = bi_build_target_custom_fields($sourceLead);
    $sourceContact = bi_extract_primary_source_contact($sourceLead);

    $targetLead = [
        'name' => bi_build_target_lead_name($sourceLead),
        'price' => (int) ($sourceLead['price'] ?? 0),
        'pipeline_id' => TARGET_AMO_PIPELINE_ID,
        'status_id' => TARGET_AMO_STATUS_ID,
        'custom_fields_values' => $mappedCustomFields,
    ];

    if (!empty($sourceContact)) {
        $targetContactId = bi_create_target_contact($sourceContact);
        $targetLead['_embedded'] = [
            'contacts' => [
                ['id' => $targetContactId],
            ]
        ];
    }

    bi_log('INFO', 'Подготовлен payload для создания сделки', [
        'source_lead_id' => $sourceLeadId,
        'classification' => $classification,
        'mapped_custom_fields' => $mappedCustomFields,
        'payload' => $targetLead,
    ]);

    $response = post_or_patch($subdomain, [$targetLead], '/api/v4/leads', $data, 'POST');
    $createdLeadId = (int) ($response['_embedded']['leads'][0]['id'] ?? 0);

    if ($createdLeadId <= 0) {
        throw new RuntimeException('Целевой портал не вернул ID созданной сделки для source #' . $sourceLeadId);
    }

    bi_log('INFO', 'Сделка создана на target-портале', [
        'source_lead_id' => $sourceLeadId,
        'target_lead_id' => $createdLeadId,
        'classification' => $classification,
    ]);

    return $createdLeadId;
}

function bi_update_target_lead(int $targetLeadId, array $sourceLead, string $classification): int
{
    global $subdomain, $data;

    $mappedCustomFields = bi_build_target_custom_fields($sourceLead);
    $payload = [[
        'id' => $targetLeadId,
        'name' => bi_build_target_lead_name($sourceLead),
        'price' => (int) ($sourceLead['price'] ?? 0),
        'pipeline_id' => TARGET_AMO_PIPELINE_ID,
        'status_id' => TARGET_AMO_STATUS_ID,
        'custom_fields_values' => $mappedCustomFields,
    ]];

    bi_log('INFO', 'Подготовлен payload для обновления существующей сделки', [
        'source_lead_id' => (int) ($sourceLead['id'] ?? 0),
        'target_lead_id' => $targetLeadId,
        'classification' => $classification,
        'mapped_custom_fields' => $mappedCustomFields,
        'payload' => $payload[0],
    ]);

    $response = post_or_patch($subdomain, $payload, '/api/v4/leads', $data, 'PATCH');
    $updatedLeadId = (int) ($response['_embedded']['leads'][0]['id'] ?? 0);

    if ($updatedLeadId <= 0) {
        throw new RuntimeException('Целевой портал не вернул ID обновленной сделки #' . $targetLeadId);
    }

    bi_log('INFO', 'Существующая сделка обновлена на target-портале', [
        'source_lead_id' => (int) ($sourceLead['id'] ?? 0),
        'target_lead_id' => $updatedLeadId,
        'classification' => $classification,
    ]);

    return $updatedLeadId;
}

function bi_create_target_contact(array $sourceContact): int
{
    global $subdomain, $data;

    $payload = bi_build_target_contact_payload($sourceContact);

    bi_log('INFO', 'Подготовлен payload для создания контакта', [
        'contact_name' => $payload['name'] ?? '',
        'payload' => $payload,
    ]);

    $response = post_or_patch($subdomain, [$payload], '/api/v4/contacts', $data, 'POST');
    $contactId = (int) ($response['_embedded']['contacts'][0]['id'] ?? 0);

    if ($contactId <= 0) {
        throw new RuntimeException('Целевой портал не вернул ID созданного контакта');
    }

    bi_log('INFO', 'Контакт создан на target-портале', [
        'target_contact_id' => $contactId,
        'contact_name' => $payload['name'] ?? '',
    ]);

    return $contactId;
}

function bi_extract_primary_source_contact(array $sourceLead): array
{
    $contactId = (int) ($sourceLead['_embedded']['contacts'][0]['id'] ?? 0);
    if ($contactId <= 0) {
        return [];
    }

    $sourceContact = bi_source_request('/api/v4/contacts/' . $contactId);
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

function bi_build_target_contact_payload(array $sourceContact): array
{
    $contact = [
        'name' => $sourceContact['name'] !== '' ? $sourceContact['name'] : 'Без имени',
    ];

    $contactFields = [];

    if (!empty($sourceContact['phones'])) {
        $contactFields[] = [
            'field_code' => 'PHONE',
            'values' => array_map(static fn (string $phone): array => [
                'value' => $phone,
                'enum_code' => 'WORK',
            ], $sourceContact['phones']),
        ];
    }

    if (!empty($sourceContact['emails'])) {
        $contactFields[] = [
            'field_code' => 'EMAIL',
            'values' => array_map(static fn (string $email): array => [
                'value' => $email,
                'enum_code' => 'WORK',
            ], $sourceContact['emails']),
        ];
    }

    $mappedContactFields = bi_build_mapped_fields(
        $sourceContact['custom_fields_values'] ?? [],
        TARGET_CONTACT_FIELD_MAP,
        'contacts'
    );

    foreach ($mappedContactFields as $mappedField) {
        $contactFields[] = $mappedField;
    }

    if ($contactFields !== []) {
        $contact['custom_fields_values'] = $contactFields;
    }

    return $contact;
}

function bi_build_target_custom_fields(array $sourceLead): array
{
    return bi_build_mapped_fields($sourceLead['custom_fields_values'] ?? [], TARGET_LEAD_FIELD_MAP, 'leads');
}

function bi_build_mapped_fields(array $sourceFields, array $fieldMap, string $entityType): array
{
    if (!is_array($sourceFields) || $sourceFields === []) {
        return [];
    }

    $targetFieldMetaById = bi_get_target_field_meta_by_id($entityType);
    $result = [];

    foreach ($sourceFields as $sourceField) {
        $sourceFieldId = (int) ($sourceField['field_id'] ?? 0);
        if ($sourceFieldId <= 0 || !isset($fieldMap[$sourceFieldId])) {
            continue;
        }

        $map = $fieldMap[$sourceFieldId];
        $targetFieldId = (int) ($map['target_field_id'] ?? 0);
        $sourceValues = $sourceField['values'] ?? [];

        if ($targetFieldId <= 0 || !is_array($sourceValues) || $sourceValues === []) {
            continue;
        }

        if (!bi_is_target_field_available_for_pipeline($targetFieldMetaById, $targetFieldId, TARGET_AMO_PIPELINE_ID)) {
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

                $targetEnumId = bi_find_target_enum_id_by_value($targetFieldMetaById, $targetFieldId, $sourceText);
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
                $sourceText = bi_normalize_name((string) ($sourceValue['value'] ?? ''));
                $textMap = is_array($map['text_map'] ?? null) ? $map['text_map'] : [];
                $targetText = (string) ($textMap[$sourceText] ?? '');
                if ($targetText === '') {
                    continue;
                }

                $targetEnumId = bi_find_target_enum_id_by_value($targetFieldMetaById, $targetFieldId, $targetText);
                if ($targetEnumId > 0) {
                    $mappedValues[] = ['enum_id' => $targetEnumId];
                }
                continue;
            }

            if (array_key_exists('value', $sourceValue)) {
                $mappedValues[] = ['value' => $sourceValue['value']];
            }
        }

        if ($mappedValues !== []) {
            $result[] = [
                'field_id' => $targetFieldId,
                'values' => $mappedValues,
            ];
        }
    }

    return $result;
}

function bi_is_target_field_available_for_pipeline(array $targetFieldMetaById, int $targetFieldId, int $pipelineId): bool
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

function bi_get_target_field_meta_by_id(string $entityType): array
{
    static $cache = [];

    if (isset($cache[$entityType])) {
        return $cache[$entityType];
    }

    global $subdomain, $data;

    $endpoint = $entityType === 'contacts' ? '/api/v4/contacts/custom_fields' : '/api/v4/leads/custom_fields';
    $response = get($subdomain, $endpoint, $data);
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

function bi_find_target_enum_id_by_value(array $targetFieldMetaById, int $targetFieldId, string $sourceValue): int
{
    $targetField = $targetFieldMetaById[$targetFieldId] ?? null;
    if (!is_array($targetField)) {
        return 0;
    }

    $enums = $targetField['enums'] ?? [];
    if (!is_array($enums)) {
        return 0;
    }

    $normalizedSource = bi_normalize_name($sourceValue);
    foreach ($enums as $enumId => $enumValue) {
        if (is_array($enumValue)) {
            $value = (string) ($enumValue['value'] ?? '');
            $id = (int) ($enumValue['id'] ?? 0);
            if ($id > 0 && $value !== '' && bi_normalize_name($value) === $normalizedSource) {
                return $id;
            }
            continue;
        }

        if (bi_normalize_name((string) $enumValue) === $normalizedSource) {
            return (int) $enumId;
        }
    }

    return 0;
}

function bi_normalize_name(string $value): string
{
    $value = mb_strtolower(trim($value), 'UTF-8');
    $value = str_replace('ё', 'е', $value);
    $value = (string) preg_replace('/\s+/u', ' ', $value);
    return $value;
}

function bi_source_request(string $path): array
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

function bi_load_state(): array
{
    if (!is_file(IMPORT_STATE_FILE)) {
        return ['processed' => []];
    }

    $raw = file_get_contents(IMPORT_STATE_FILE);
    $decoded = json_decode((string) $raw, true);

    return is_array($decoded) ? $decoded : ['processed' => []];
}

function bi_save_state(array $state): void
{
    $json = json_encode($state, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    if ($json !== false) {
        file_put_contents(IMPORT_STATE_FILE, $json . PHP_EOL, LOCK_EX);
    }
}

function bi_log(string $level, string $message, array $context = []): void
{
    $logDir = dirname(BATCH_IMPORT_LOG_FILE);
    if (!is_dir($logDir)) {
        @mkdir($logDir, 0755, true);
    }

    $line = '[' . date('Y-m-d H:i:s') . ']';
    $line .= ' [' . strtoupper($level) . ']';
    $line .= ' [' . BATCH_IMPORT_SOURCE . '] ' . $message;

    if ($context !== []) {
        $line .= "\n" . json_encode($context, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
    }

    $line .= "\n" . str_repeat('-', 80) . "\n";
    @file_put_contents(BATCH_IMPORT_LOG_FILE, $line, FILE_APPEND | LOCK_EX);
}

function bi_respond(array $payload, int $statusCode): void
{
    if (!headers_sent()) {
        header('Content-Type: application/json; charset=utf-8');
        http_response_code($statusCode);
    }

    echo json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
}
