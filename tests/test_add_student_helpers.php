<?php

declare(strict_types=1);

require_once __DIR__ . '/bootstrap.php';

$sourceFile = tests_project_root() . '/add_student.php';
tests_load_functions($sourceFile, [
    'normalize_extra_field_name',
    'normalize_holly_birth_date',
    'normalize_holly_custom_date',
    'normalize_holly_email',
    'build_student_extra_fields_from_post_data',
    'sanitize_holly_name',
    'split_full_name',
    'build_agent_contacts_payload',
    'build_ind_client_params_payload',
    'build_extra_fields_update_payload',
]);

tests_assert_same('', normalize_holly_email(''), 'Empty email should stay empty.');
tests_assert_same('', normalize_holly_email('   '), 'Whitespace email should stay empty.');
tests_assert_same('', normalize_holly_email('.'), 'Single dot email must be ignored.');
tests_assert_same('', normalize_holly_email(' . '), 'Trimmed single dot email must be ignored.');
tests_assert_same('user@example.com', normalize_holly_email(' user@example.com '), 'Valid email should be trimmed.');

tests_assert_same('2026-08-31', normalize_holly_birth_date('31.08.2026'), 'Birth date dd.mm.yyyy should normalize.');
tests_assert_same('2026-08-31', normalize_holly_birth_date('31/08/2026'), 'Birth date dd/mm/yyyy should normalize.');
tests_assert_same('2026-08-31', normalize_holly_birth_date('1788134400'), 'Birth date unix timestamp should normalize.');
tests_assert_same('31.08.2026', normalize_holly_custom_date('2026-08-31'), 'Custom date yyyy-mm-dd should normalize.');
tests_assert_same('31.08.2026', normalize_holly_custom_date('1788134400'), 'Custom date unix timestamp should normalize.');

tests_assert_same("O'Connor", sanitize_holly_name("O'Connor"), 'Valid apostrophe should be preserved.');
tests_assert_same('Иван', sanitize_holly_name('Иван123'), 'Digits should be removed from Holly names.');

$fullName = split_full_name('Анна Иванова Сергеевна');
tests_assert_same('Анна', $fullName['firstName'], 'First name should be extracted.');
tests_assert_same('Иванова', $fullName['lastName'], 'Last name should be extracted.');
tests_assert_same('Сергеевна', $fullName['middleName'], 'Middle name should be extracted.');

$extraFields = build_student_extra_fields_from_post_data([
    'childName' => 'Петя Иванов',
    'Дата рождения' => '31.08.2026',
    'hollyPackage' => '8 занятий',
    'hollyComboActive' => 'Да',
    'hollySecondPaymentDue' => '2026-09-15',
]);
tests_assert_count(5, $extraFields, 'Expected child and Holly extra fields to be generated.');
tests_assert_same('ФИО ребенка', $extraFields[0]['name'], 'Child name extra field should be generated first.');
tests_assert_same('Петя Иванов', $extraFields[0]['value'], 'Child name should be preserved.');
tests_assert_same('Дата рождения', $extraFields[1]['name'], 'Birth date extra field should be generated.');
tests_assert_same('2026-08-31', $extraFields[1]['value'], 'Birth date should be normalized for Holly.');
tests_assert_same('Комбо Актив (индив)', $extraFields[3]['name'], 'Combo Active should map to Holly field.');
tests_assert_same('Срок оплаты 50/50 (вторая часть) индив', $extraFields[4]['name'], 'Second payment due field should be added.');
tests_assert_same('15.09.2026', $extraFields[4]['value'], 'Second payment due should be normalized to dd.mm.yyyy.');

$agentsPayload = build_agent_contacts_payload([
    'Agents' => [[
        'FirstName' => 'Старый',
        'LastName' => 'Родитель',
        'WhoIs' => 'Родитель',
        'Mobile' => '+79990000000',
        'Phone' => '+79990000000',
        'EMail' => 'old@example.com',
        'IsCustomer' => true,
    ]],
], [
    'parentName' => 'Новый Родитель',
    'parentPhone' => '+79991112233',
    'parentEmail' => '.',
    'parentEmergencyPhone' => '+79994445566',
], 42);
tests_assert_same(42, $agentsPayload['studentClientId'], 'Agent payload should keep studentClientId.');
tests_assert_count(2, $agentsPayload['agents'], 'Parent and emergency contact should be present.');
tests_assert_same('+79991112233', $agentsPayload['agents'][0]['mobile'], 'Existing parent should be updated with new phone.');
tests_assert_true(!isset($agentsPayload['agents'][0]['eMail']) || $agentsPayload['agents'][0]['eMail'] !== '.', 'Dot email must not leak into parent agent payload.');
tests_assert_same('Экстренный', $agentsPayload['agents'][1]['firstName'], 'Emergency contact should be appended.');

$indPayload = build_ind_client_params_payload([
    'discipline' => 'Китайский',
    'level' => 'A2',
], 77);
tests_assert_same(77, $indPayload['studentClientId'], 'Ind params should include client id.');
tests_assert_same('Китайский', $indPayload['discipline'], 'Discipline should be passed through.');
tests_assert_same('A2', $indPayload['level'], 'Level should be passed through.');
tests_assert_same(null, build_ind_client_params_payload([], 77), 'Empty ind params should return null.');

$extraUpdate = build_extra_fields_update_payload([
    'ExtraFields' => [
        ['Name' => 'Уровень языка из OkiDoki', 'Value' => 'A1'],
        ['Name' => 'Другое', 'Value' => 'X'],
    ],
], [
    'sourceSystem' => 'okidoki',
    'level' => 'B1',
], 91);
tests_assert_same(91, $extraUpdate['studentClientId'], 'Extra fields payload should include client id.');
tests_assert_count(2, $extraUpdate['fields'], 'Existing fields should be preserved.');
tests_assert_same('B1', $extraUpdate['fields'][0]['value'], 'OkiDoki level should be updated in-place.');

echo "OK\n";
