<?php
declare(strict_types=1);

require_once __DIR__ . '/amo_func.php';

$leadId = 29505455;
$lead = get($subdomain, "/api/v4/leads/{$leadId}?with=contacts", $data);

$contactId = $lead['_embedded']['contacts'][0]['id'] ?? null;
$contact = null;
if ($contactId) {
    $contact = get($subdomain, "/api/v4/contacts/{$contactId}", $data);
}

function findFieldValue(array $entity, int $fieldId): ?string
{
    foreach ($entity['custom_fields_values'] ?? [] as $field) {
        if ((int) ($field['field_id'] ?? 0) !== $fieldId) {
            continue;
        }

        $value = $field['values'][0]['value'] ?? null;
        return $value === null ? null : (string) $value;
    }

    return null;
}

header('Content-Type: application/json; charset=utf-8');
echo json_encode([
    'lead_id' => $leadId,
    'contact_id' => $contactId,
    'lead_telegram' => findFieldValue($lead, 1630032),
    'contact_telegram' => $contact ? findFieldValue($contact, 1630032) : null,
    'lead_name' => $lead['name'] ?? null,
    'contact_name' => $contact['name'] ?? null,
    'lead_fields' => array_map(static function (array $field): array {
        return [
            'field_id' => $field['field_id'] ?? null,
            'name' => $field['field_name'] ?? $field['name'] ?? null,
            'value' => $field['values'][0]['value'] ?? null,
        ];
    }, $lead['custom_fields_values'] ?? []),
    'contact_fields' => array_map(static function (array $field): array {
        return [
            'field_id' => $field['field_id'] ?? null,
            'name' => $field['field_name'] ?? $field['name'] ?? null,
            'value' => $field['values'][0]['value'] ?? null,
        ];
    }, $contact['custom_fields_values'] ?? []),
], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
