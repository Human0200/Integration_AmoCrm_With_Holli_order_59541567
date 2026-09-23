<?php

declare(strict_types=1);

require_once __DIR__ . '/bootstrap.php';

if (!function_exists('get_config')) {
    function get_config($key = null)
    {
        $config = $GLOBALS['tests_yandex_config'];
        if ($key === null) {
            return $config;
        }

        $parts = explode('.', (string) $key);
        $value = $config;
        foreach ($parts as $part) {
            if (!is_array($value) || !array_key_exists($part, $value)) {
                return null;
            }
            $value = $value[$part];
        }

        return $value;
    }
}

if (!function_exists('yandex_forms_fetch_answer')) {
    function yandex_forms_fetch_answer(?int $answerId = null, ?string $answerKey = null): array
    {
        return [
            'created' => '2026-08-31T12:00:00+03:00',
            'survey' => [
                'id' => '6a738889381ea61914181597',
                'name' => 'Тестовая форма',
                'items' => [
                    ['label' => 'Имя', 'value' => 'Анна'],
                    ['label' => 'Согласие', 'value' => true],
                    ['slug' => 'amo_lead_id', 'value' => '29450109'],
                ],
            ],
        ];
    }
}

$storageDir = sys_get_temp_dir() . '/codex_yandex_forms_tests_' . getmypid();
@mkdir($storageDir, 0777, true);

$GLOBALS['tests_yandex_config'] = [
    'yandex_forms' => [
        'lead_param_name' => 'amo_lead_id',
        'portal_param_name' => 'amo_portal',
        'public_base_url' => 'https://srm.chinatutor.ru',
        'storage_dir' => $storageDir,
    ],
    'amocrm_portals' => [
        'directorchinatutorru' => [
            'subdomain' => 'directorchinatutorru',
            'results_field_id' => 1639409,
        ],
        'secondary' => [
            'subdomain' => 'supportchinatutorru',
            'results_field_id' => 1902565,
        ],
    ],
];

$sourceFile = tests_project_root() . '/yandex_forms_common.php';
tests_load_functions($sourceFile, [
    'yandex_forms_config',
    'yandex_forms_extract_query_params',
    'yandex_forms_normalize_portal_key',
    'yandex_forms_resolve_portal_key',
    'yandex_forms_portal_config',
    'yandex_forms_storage_dir',
    'yandex_forms_results_dir',
    'yandex_forms_delivery_dir',
    'yandex_forms_get_header_value',
    'yandex_forms_enrich_payload_with_headers',
    'yandex_forms_extract_forms_from_response',
    'yandex_forms_form_public_url',
    'yandex_forms_normalize_form',
    'yandex_forms_extract_lead_id_from_payload',
    'yandex_forms_extract_answer_rows',
    'yandex_forms_build_snapshot',
    'yandex_forms_answer_url',
    'yandex_forms_store_snapshot',
    'yandex_forms_load_snapshot',
    'yandex_forms_public_result_url',
]);

$_GET = ['amo_portal' => 'supportchinatutorru'];
$_SERVER['HTTP_X_FORM_ANSWER_ID'] = '2486802909';
$_SERVER['HTTP_X_FORM_ID'] = '6a738889381ea61914181597';

tests_assert_same(['amo_lead_id' => '29450109'], yandex_forms_extract_query_params(['query_params' => 'amo_lead_id=29450109']), 'Query string should parse into array.');
tests_assert_same(['amo_lead_id' => '29450109'], yandex_forms_extract_query_params(['query_params' => '{"amo_lead_id":"29450109"}']), 'JSON query params should parse into array.');

tests_assert_same('directorchinatutorru', yandex_forms_normalize_portal_key('directorchinatutorru'), 'Portal key should stay unchanged.');
tests_assert_same('secondary', yandex_forms_normalize_portal_key('supportchinatutorru'), 'Portal subdomain should resolve to configured key.');
tests_assert_same('secondary', yandex_forms_resolve_portal_key(['query_params' => ['amo_portal' => 'supportchinatutorru']]), 'Portal should resolve from payload query params.');

$portalConfig = yandex_forms_portal_config('secondary');
tests_assert_same('secondary', $portalConfig['portal_key'], 'Portal config should include resolved portal key.');

$normalizedForm = yandex_forms_normalize_form([
    'id' => '6a738889381ea61914181597',
    'name' => 'Тестовая форма',
    'public_url' => 'https://forms.yandex.ru/u/6a738889381ea61914181597/',
], 29450109, null, 'directorchinatutorru');
tests_assert_same('Тестовая форма', $normalizedForm['name'], 'Form name should be preserved.');
tests_assert_contains('amo_lead_id=29450109', $normalizedForm['generated_link'], 'Generated form link should include lead id.');
tests_assert_contains('amo_portal=directorchinatutorru', $normalizedForm['generated_link'], 'Generated form link should include portal.');

tests_assert_same(29450109, yandex_forms_extract_lead_id_from_payload([
    'query_params' => ['amo_lead_id' => '29450109'],
]), 'Lead id should resolve from query params.');
tests_assert_same(29450109, yandex_forms_extract_lead_id_from_payload([
    'survey' => ['items' => [['slug' => 'amo_lead_id', 'value' => '29450109']]],
]), 'Lead id should resolve from survey items.');
tests_assert_same(29450109, yandex_forms_extract_lead_id_from_payload([
    'answers_json' => '{"amo_lead_id":{"value":"29450109"}}',
]), 'Lead id should resolve from answers_json.');

$rows = yandex_forms_extract_answer_rows([
    'survey' => [
        'items' => [
            ['label' => 'Имя', 'value' => 'Анна'],
            ['label' => 'Языки', 'value' => ['Китайский', 'Корейский']],
            ['label' => 'Согласие', 'value' => true],
        ],
    ],
]);
tests_assert_count(3, $rows, 'Answer rows should be extracted from survey items.');
tests_assert_same('Китайский, Корейский', $rows[1]['value'], 'Array answers should be joined.');
tests_assert_same('Да', $rows[2]['value'], 'Boolean answers should be localized.');

$snapshot = yandex_forms_build_snapshot([
    'answer_id' => 2486802909,
    'query_params' => ['amo_lead_id' => '29450109', 'amo_portal' => 'supportchinatutorru'],
]);
tests_assert_same(29450109, $snapshot['lead_id'], 'Snapshot should resolve lead id.');
tests_assert_same('secondary', $snapshot['portal_key'], 'Snapshot should resolve portal key.');
tests_assert_same('Тестовая форма', $snapshot['survey_name'], 'Snapshot should preserve survey name.');
tests_assert_count(3, $snapshot['rows'], 'Snapshot should include extracted answer rows.');

$answerUrl = yandex_forms_answer_url($snapshot);
tests_assert_same('https://forms.yandex.ru/cloud/admin/6a738889381ea61914181597/answers/2486802909', $answerUrl, 'Answer URL should be assembled from survey and answer ids.');

$publicId = yandex_forms_store_snapshot($snapshot);
tests_assert_true($publicId !== '', 'Public snapshot id should be generated.');
$loadedSnapshot = yandex_forms_load_snapshot($publicId);
tests_assert_same($snapshot['lead_id'], $loadedSnapshot['lead_id'], 'Stored snapshot should load back from disk.');
tests_assert_same('https://srm.chinatutor.ru/yandex_forms_result.php?id=' . $publicId, yandex_forms_public_result_url($publicId), 'Public result URL should use configured base URL.');

echo "OK\n";
