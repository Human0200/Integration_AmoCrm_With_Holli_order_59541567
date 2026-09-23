<?php

declare(strict_types=1);

require_once __DIR__ . '/bootstrap.php';

$sourceFile = tests_project_root() . '/telegram_consultation_webhook.php';
tests_load_functions($sourceFile, [
    'extractLeadIdFromWebhook',
    'extractDateTimeField',
    'extractPositiveInt',
]);

tests_assert_same(31369877, extractLeadIdFromWebhook([
    'leads' => [
        'update' => [['id' => '31369877']],
        'add' => [['id' => '999']],
    ],
]), 'Telegram webhook helper should prefer update event lead id.');
tests_assert_same(123, extractLeadIdFromWebhook([
    'leads' => [
        'status' => [['id' => '123']],
    ],
]), 'Telegram webhook helper should support status events.');
tests_assert_same(null, extractLeadIdFromWebhook(['contacts' => []]), 'Telegram webhook helper should return null without lead payload.');

tests_assert_same(1788138000, extractDateTimeField([
    'custom_fields_values' => [
        ['field_id' => 1583083, 'values' => [['value' => '1788138000']]],
    ],
], 1583083), 'Timestamp consultation field should be returned as int.');
tests_assert_true(extractDateTimeField([
    'custom_fields_values' => [
        ['field_id' => 1583083, 'values' => [['value' => '2026-08-31 12:00:00']]],
    ],
], 1583083) !== null, 'String consultation date should be parsed by strtotime.');
tests_assert_same(null, extractDateTimeField([
    'custom_fields_values' => [
        ['field_id' => 999, 'values' => [['value' => '1788138000']]],
    ],
], 1583083), 'Missing field id should return null.');

tests_assert_same(932, extractPositiveInt('932'), 'Positive integer string should be parsed.');
tests_assert_same(null, extractPositiveInt(''), 'Empty thread id should be ignored.');
tests_assert_same(null, extractPositiveInt('abc'), 'Non-numeric thread id should be ignored.');

echo "OK\n";
