<?php

declare(strict_types=1);

const SOURCE_AMO_BASE_URL = 'https://supportchinatutorru.amocrm.ru';
const SOURCE_AMO_ACCESS_TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6ImM2MDc4MDY1ZTczN2IwOWU1MTcxMDY5ZjUzZmIyN2QwNzBjMTA3YzIyODI0NzAxNzAzNjdhZGI2NzlhM2MwOWEyNzM1NGI5ZDk5ZjE1ZDMzIn0.eyJhdWQiOiIwNTQ0ZDY3NC00MmI4LTRiYTMtOTBlZC02OTM4MGFkMzNjNWQiLCJqdGkiOiJjNjA3ODA2NWU3MzdiMDllNTE3MTA2OWY1M2ZiMjdkMDcwYzEwN2MyMjgyNDcwMTcwMzY3YWRiNjc5YTNjMDlhMjczNTRiOWQ5OWYxNWQzMyIsImlhdCI6MTc3NTY2NDY0NSwibmJmIjoxNzc1NjY0NjQ1LCJleHAiOjE3ODAxODU2MDAsInN1YiI6IjEyNjQyMzE0IiwiZ3JhbnRfdHlwZSI6IiIsImFjY291bnRfaWQiOjMyNDk1NjI2LCJiYXNlX2RvbWFpbiI6ImFtb2NybS5ydSIsInZlcnNpb24iOjIsInNjb3BlcyI6WyJwdXNoX25vdGlmaWNhdGlvbnMiLCJmaWxlcyIsImNybSIsImZpbGVzX2RlbGV0ZSIsIm5vdGlmaWNhdGlvbnMiXSwiaGFzaF91dWlkIjoiMGFiYmNhYWUtYzUzNi00NzMwLWEyODAtNDdjNjAwMjBkMGJmIiwiYXBpX2RvbWFpbiI6ImFwaS1iLmFtb2NybS5ydSJ9.GWXm7Z4v0eXQcb9w3SrlTdeY31GGzJf4Ta7EZZune6O61j520bu-rQKyLeUhJNjZz4tPDLBbf7l5_P-60QGAg_aW-JwCwExpMb04_0FLZIl4GcclB_dnT9zoccvsrXe58bNl50du4hiAhan58GWb51K9zM0BI8A0cOB9Hasytno1dJ_eLF9euyYlP5d_yRqV-5TsryUCg6PDwQaKgfZsjOh-bHY60vog6NHuM5u66BN6l_4XI44tTARWYDySRx9UpwnsQLkCgDUfPKHE9ij4D92-hJJSkUubn9jygblVasxoOz0oCH65Rx38Jo2wHuZ21RjgGXOp-Dwb5gBOW3zoxA';

header('Content-Type: text/plain; charset=utf-8');

function amo_api_get(string $path): array
{
    $url = rtrim(SOURCE_AMO_BASE_URL, '/') . $path;

    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => $url,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_HTTPGET => true,
        CURLOPT_HTTPHEADER => [
            'Authorization: Bearer ' . SOURCE_AMO_ACCESS_TOKEN,
            'Content-Type: application/json',
        ],
        CURLOPT_TIMEOUT => 60,
    ]);

    $response = curl_exec($ch);
    $httpCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error = curl_error($ch);
    curl_close($ch);

    if ($error !== '') {
        throw new RuntimeException('cURL error: ' . $error);
    }

    $decoded = json_decode((string) $response, true);

    if ($httpCode < 200 || $httpCode >= 300) {
        throw new RuntimeException("amo API HTTP {$httpCode}: " . mb_substr((string) $response, 0, 1500));
    }

    return is_array($decoded) ? $decoded : [];
}

function norm(string $s): string
{
    $s = mb_strtolower(trim($s), 'UTF-8');
    $s = str_replace('ё', 'е', $s);
    return preg_replace('/\s+/u', ' ', $s) ?? $s;
}

try {
    echo "=== ACCOUNT ===\n";
    $account = amo_api_get('/api/v4/account');
    echo "ID: " . ($account['id'] ?? '') . "\n";
    echo "NAME: " . ($account['name'] ?? '') . "\n";
    echo "SUBDOMAIN: " . ($account['subdomain'] ?? '') . "\n";

    echo "\n=== USERS ===\n";
    $users = amo_api_get('/api/v4/users');
    foreach (($users['_embedded']['users'] ?? []) as $user) {
        echo ($user['id'] ?? '') . " | " . ($user['name'] ?? '') . " | " . ($user['email'] ?? '') . "\n";
    }

    echo "\n=== PIPELINES / STATUSES ===\n";
    $pipelines = amo_api_get('/api/v4/leads/pipelines');
    foreach (($pipelines['_embedded']['pipelines'] ?? []) as $pipeline) {
        echo "\nPIPELINE: " . ($pipeline['id'] ?? '') . " | " . ($pipeline['name'] ?? '') . "\n";
        foreach (($pipeline['_embedded']['statuses'] ?? []) as $status) {
            echo "  STATUS: " . ($status['id'] ?? '') . " | " . ($status['name'] ?? '') . "\n";
        }
    }

    echo "\n=== LEAD CUSTOM FIELDS ===\n";
    $leadFields = amo_api_get('/api/v4/leads/custom_fields');
    foreach (($leadFields['_embedded']['custom_fields'] ?? []) as $field) {
        $name = (string) ($field['name'] ?? '');
        $n = norm($name);

        if (
            str_contains($n, 'язык') ||
            str_contains($n, 'уровень') ||
            str_contains($n, 'вид занятий') ||
            str_contains($n, 'тип обучения') ||
            str_contains($n, 'филиал') ||
            str_contains($n, 'ответственный') ||
            str_contains($n, 'дата рождения') ||
            str_contains($n, 'языковой клуб') ||
            str_contains($n, 'комбо') ||
            str_contains($n, 'холи') ||
            str_contains($n, 'хх') ||
            str_contains($n, 'амо')
        ) {
            echo ($field['id'] ?? '') . " | " . $name . " | type=" . ($field['type'] ?? '') . "\n";
        }
    }

    echo "\n=== CONTACT CUSTOM FIELDS ===\n";
    $contactFields = amo_api_get('/api/v4/contacts/custom_fields');
    foreach (($contactFields['_embedded']['custom_fields'] ?? []) as $field) {
        $name = (string) ($field['name'] ?? '');
        $n = norm($name);

        if (
            str_contains($n, 'телефон') ||
            str_contains($n, 'email') ||
            str_contains($n, 'e-mail') ||
            str_contains($n, 'должность') ||
            str_contains($n, 'кем является')
        ) {
            echo ($field['id'] ?? '') . " | " . $name . " | type=" . ($field['type'] ?? '') . "\n";
        }
    }
} catch (Throwable $e) {
    echo "ERROR: " . $e->getMessage() . "\n";
}
