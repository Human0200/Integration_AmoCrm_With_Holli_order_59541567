<?php

declare(strict_types=1);

require_once __DIR__ . '/bootstrap.php';

if (!function_exists('log_info')) {
    function log_info(string $message, $context = null, string $source = ''): void {}
}
if (!function_exists('log_warning')) {
    function log_warning(string $message, $context = null, string $source = ''): void {}
}
if (!function_exists('log_error')) {
    function log_error(string $message, $context = null, string $source = ''): void {}
}
if (!function_exists('extractContactData')) {
    function extractContactData(int $contactId): array
    {
        return [
            'parentName' => 'Мария Петрова',
            'parentPhone' => '+79990001122',
            'parentEmail' => 'maria@example.com',
            'childName' => 'Петя Петров',
            'birthDate' => '2026-08-31',
        ];
    }
}

foreach ([
    'AMO_FIELD_DISCIPLINE' => 1575217,
    'AMO_FIELD_LEVEL' => 1576357,
    'AMO_FIELD_LEARNING_TYPE' => 1575221,
    'AMO_FIELD_LESSON_KIND' => 1575317,
    'AMO_FIELD_LEARNING_FORMAT' => 1606825,
    'AMO_FIELD_MATURITY' => 1575213,
    'AMO_FIELD_OFFICE_OR_COMPANY' => 1596219,
    'AMO_FIELD_RESPONSIBLE_USER' => 1590693,
    'AMO_FIELD_PROFILE_LINK' => 1630807,
    'AMO_FIELD_PACKAGE' => 1639037,
    'AMO_FIELD_LESSONS_COUNT' => 1639039,
    'AMO_FIELD_PACKAGE_TERM' => 1639041,
    'AMO_FIELD_INTENSITY' => 1639043,
    'AMO_FIELD_LESSON_DURATION' => 1639047,
    'AMO_FIELD_LESSON_LOCATION' => 1639049,
    'AMO_FIELD_TEACHER' => 1639051,
    'AMO_FIELD_SLOT_FIXED' => 1639053,
    'AMO_FIELD_SCHEDULE' => 1639055,
    'AMO_FIELD_DISCOUNT' => 1639057,
    'AMO_FIELD_VIP' => 1639059,
    'AMO_FIELD_LESSON_PRICE' => 1639061,
    'AMO_FIELD_TOTAL_PRICE' => 1639063,
    'AMO_FIELD_COMBO_ACTIVE' => 1639065,
    'AMO_FIELD_LANGUAGE_CLUB' => 1639067,
    'AMO_FIELD_TRANSFER_LIMIT' => 1639069,
    'AMO_FIELD_FREE_PAUSE' => 1639071,
    'AMO_FIELD_SECOND_PAYMENT_DUE' => 1639073,
    'AMO_CONTACT_FIELD_TELEGRAM' => 1630032,
    'AMO_DEALS_FIELD_NAME' => 'Сделки АМО',
] as $name => $value) {
    if (!defined($name)) {
        define($name, $value);
    }
}

global $subdomain;
$subdomain = 'directorchinatutorru';

$sourceFile = tests_project_root() . '/index.php';
tests_load_functions($sourceFile, [
    'normalizeOkiLevelForAmo',
    'normalizeOkiLanguageForAmo',
    'normalizeOkiDateForAmo',
    'normalizeHollyCustomDateValue',
    'extractLeadWebhookContext',
    'extractLeadCustomFields',
    'buildStudentDataFromLead',
    'buildHollyExtraFieldsFromStudentData',
    'isAmoDealField',
    'mergeAmoDealLinks',
    'parseExistingLinks',
    'linkExists',
    'extractUrlFromLink',
    'normalizeUrl',
]);

tests_assert_same('A1', normalizeOkiLevelForAmo('beginner'), 'Oki level mapping should normalize beginner.');
tests_assert_same('B1', normalizeOkiLevelForAmo('pre-intermediate'), 'Oki level mapping should normalize pre-intermediate.');
tests_assert_same('Китайский', normalizeOkiLanguageForAmo('chinese'), 'Language mapping should normalize english names.');
tests_assert_same('РКИ', normalizeOkiLanguageForAmo('рки'), 'Language mapping should preserve supported acronyms.');
tests_assert_same('2026-08-31', normalizeOkiDateForAmo('31.08.2026'), 'Oki date should normalize dd.mm.yyyy.');
tests_assert_same('31.08.2026', normalizeHollyCustomDateValue('2026-08-31'), 'Holly custom date should normalize yyyy-mm-dd.');

$ctx = extractLeadWebhookContext(['leads' => ['update' => [['id' => '123', 'last_modified' => '999']]]]);
tests_assert_same(123, $ctx['lead_id'], 'Webhook context should extract update lead id.');
tests_assert_same('update', $ctx['event_type'], 'Webhook context should preserve event type.');
tests_assert_same('999', $ctx['last_modified'], 'Webhook context should preserve last_modified.');
tests_assert_same(null, extractLeadWebhookContext(['contacts' => []]), 'Webhook context should return null for unsupported payloads.');

$leadFields = extractLeadCustomFields([
    ['field_id' => AMO_FIELD_DISCIPLINE, 'values' => [['value' => 'Китайский']]],
    ['field_id' => AMO_FIELD_LEVEL, 'values' => [['value' => 'A2']]],
    ['field_id' => AMO_FIELD_LEARNING_TYPE, 'values' => [['value' => 'Группа']]],
    ['field_id' => AMO_FIELD_LESSON_KIND, 'values' => [['value' => 'Индивидуально']]],
    ['field_id' => AMO_FIELD_LEARNING_FORMAT, 'values' => [['value' => 'Индивидуально онлайн']]],
    ['field_id' => AMO_FIELD_PACKAGE, 'values' => [['value' => '12']]],
    ['field_id' => AMO_FIELD_COMBO_ACTIVE, 'values' => [['value' => 'Да']]],
    ['field_id' => AMO_FIELD_SECOND_PAYMENT_DUE, 'values' => [['value' => '1788134400']]],
    ['field_id' => AMO_FIELD_PROFILE_LINK, 'values' => [['value' => 'https://evrasia20.t8s.ru/Profile/28111']]],
]);
tests_assert_same('Китайский', $leadFields['discipline'], 'Discipline field should map from amo.');
tests_assert_same('Индивидуально онлайн', $leadFields['learningType'], 'Learning type should be overridden by the more specific format.');
tests_assert_same('12', $leadFields['hollyPackage'], 'Holly package should map from amo custom field.');
tests_assert_same('Да', $leadFields['hollyComboActive'], 'Combo Active should map from amo custom field.');
tests_assert_same(28111, $leadFields['existing_profile_id'], 'Existing Holly profile should be extracted from profile link.');

$studentData = buildStudentDataFromLead([
    'custom_fields_values' => [
        ['field_id' => AMO_FIELD_DISCIPLINE, 'values' => [['value' => 'Китайский']]],
        ['field_id' => AMO_FIELD_LEARNING_FORMAT, 'values' => [['value' => 'Группа онлайн']]],
    ],
    '_embedded' => [
        'contacts' => [['id' => 555]],
    ],
], 31369877);
tests_assert_same('В наборе', $studentData['Status'], 'Student data should contain default Holly status.');
tests_assert_same('https://directorchinatutorru.amocrm.ru/leads/detail/31369877', $studentData['link'], 'Student data should contain amo lead link.');
tests_assert_same('Группа онлайн', $studentData['learningType'], 'Student data should merge lead custom fields.');
tests_assert_same('Мария Петрова', $studentData['parentName'], 'Student data should merge contact data.');

$hollyExtraFields = buildHollyExtraFieldsFromStudentData([
    'hollyLessonPrice' => '2000',
    'hollyLanguageClub' => 'Нет',
    'hollySecondPaymentDue' => '1788134400',
]);
tests_assert_count(3, $hollyExtraFields, 'Holly extra fields should include all non-empty mapped values.');
tests_assert_same('Срок оплаты 50/50 (вторая часть) индив', $hollyExtraFields[2]['name'], 'Second payment due field name should match Holly.');
tests_assert_same('31.08.2026', $hollyExtraFields[2]['value'], 'Second payment due should be converted from timestamp.');

tests_assert_true(isAmoDealField('Сделки АМО'), 'Canonical amo deals field should be recognized.');
tests_assert_true(isAmoDealField('Ссылки amo'), 'Alternative amo deals field name should be recognized.');

$existing = '<a href="https://directorchinatutorru.amocrm.ru/leads/detail/1">1</a><br><a href="https://directorchinatutorru.amocrm.ru/leads/detail/2/">2</a>';
$merged = mergeAmoDealLinks($existing, '<a href="https://directorchinatutorru.amocrm.ru/leads/detail/2">2</a>');
tests_assert_count(2, parseExistingLinks($merged), 'Duplicate amo deal links should not be appended.');
$merged = mergeAmoDealLinks($existing, '<a href="https://directorchinatutorru.amocrm.ru/leads/detail/3">3</a>');
tests_assert_count(3, parseExistingLinks($merged), 'New amo deal links should be appended.');
tests_assert_true(linkExists(parseExistingLinks($merged), 'https://directorchinatutorru.amocrm.ru/leads/detail/3/'), 'Link existence should ignore trailing slash.');
tests_assert_same('https://directorchinatutorru.amocrm.ru/leads/detail/3', extractUrlFromLink('<a href="https://directorchinatutorru.amocrm.ru/leads/detail/3">3</a>'), 'URL should be extracted from HTML links.');
tests_assert_same('https://directorchinatutorru.amocrm.ru/leads/detail/3', normalizeUrl('https://directorchinatutorru.amocrm.ru/leads/detail/3/'), 'URL normalization should trim trailing slash.');

echo "OK\n";
