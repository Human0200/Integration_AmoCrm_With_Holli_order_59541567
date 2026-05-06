<?php

declare(strict_types=1);

require_once __DIR__ . '/config.php';
require_once __DIR__ . '/logger.php';
require_once __DIR__ . '/amo_func.php';

const COORDINATORS_SOURCE = 'amo_coordinators_hook.php';
// Стандартные поля контакта amoCRM.
const AMO_CONTACT_FIELD_PHONE = 1138327;
const AMO_CONTACT_FIELD_EMAIL = 1138329;

// Поля сделки amoCRM из ТЗ.
const AMO_FIELD_DISCIPLINE = 1575217;
const AMO_FIELD_LEVEL = 1576357;
const AMO_FIELD_LEARNING_FORMAT = 1575317;
const AMO_FIELD_OFFICE = 1596219;
const AMO_FIELD_RESPONSIBLE = 1590693;
const AMO_FIELD_HOLLY_LINK = 1630807;
// Ниже впишите недостающие ID полей amoCRM, когда они будут известны.
const AMO_FIELD_BIRTHDATE = 0;
const AMO_FIELD_LANGUAGE_CLUB = 0;
const AMO_FIELD_LANGUAGE_CLUB_UNTIL = 0;
const AMO_FIELD_COMBO_PLATFORM_OS = 0;
const AMO_CONTACT_FIELD_POSITION = 0;

// ID пользовательских полей Hollyhop.
const HOLLY_FIELD_RESPONSIBLE = 0;
const HOLLY_FIELD_AMO_OS = 0;

// Воронка "Общая", если координатор не распознан.
const AMO_COORD_GENERAL_PIPELINE_ID = 0;
const AMO_COORD_GENERAL_STATUS_ID = 0;

// Маршрутизация координатора Алина.
const AMO_COORD_ALINA_VALUE = 'Алина';
const AMO_COORD_ALINA_USER_ID = 0;
const AMO_COORD_ALINA_PIPELINE_ID = 0;
const AMO_COORD_ALINA_STATUS_ID = 0;

// Маршрутизация координатора Ирина.
const AMO_COORD_IRINA_VALUE = 'Ирина';
const AMO_COORD_IRINA_USER_ID = 0;
const AMO_COORD_IRINA_PIPELINE_ID = 0;
const AMO_COORD_IRINA_STATUS_ID = 0;

// Маршрутизация координатора Шабо.
const AMO_COORD_SHABO_VALUE = 'Шабо';
const AMO_COORD_SHABO_USER_ID = 0;
const AMO_COORD_SHABO_PIPELINE_ID = 0;
const AMO_COORD_SHABO_STATUS_ID = 0;

header('Access-Control-Allow-Methods: POST, OPTIONS');
header('Access-Control-Allow-Headers: Content-Type');
header('Content-Type: application/json; charset=utf-8');

if (($_SERVER['REQUEST_METHOD'] ?? 'GET') === 'OPTIONS') {
    http_response_code(200);
    exit;
}

if (($_SERVER['REQUEST_METHOD'] ?? 'GET') !== 'POST') {
    http_response_code(405);
    echo json_encode([
        'success' => false,
        'error' => 'Метод не поддерживается. Используйте POST.'
    ], JSON_UNESCAPED_UNICODE);
    exit;
}

try {
    $payload = getCoordinatorRequestPayload();
    coordinator_log_info('Получен webhook координаторов', [
        'payload_keys' => array_keys($payload)
    ]);

    $student = fetchHollyStudentForCoordinator($payload);
    $studentData = normalizeCoordinatorStudent($student, $payload);
    $routing = resolveCoordinatorRouting($studentData['responsible']);

    coordinator_log_info('Подготовлены данные студента и маршрутизация', [
        'student' => maskCoordinatorStudentData($studentData),
        'routing' => $routing
    ]);

    $amoLeadId = parseAmoLeadIdFromString($studentData['amoOsLink']);
    $amoLeadUrl = '';
    $action = '';

    if ($amoLeadId > 0) {
        updateCoordinatorLead($amoLeadId, $studentData, $routing);
        $amoLeadUrl = buildAmoLeadUrl($amoLeadId);
        $action = 'updated';
        coordinator_log_info('Обновлена существующая сделка amo ОС', [
            'amo_lead_id' => $amoLeadId
        ]);
    } else {
        $createdLead = createCoordinatorLead($studentData, $routing);
        $amoLeadId = (int) ($createdLead['id'] ?? 0);
        if ($amoLeadId <= 0) {
            throw new RuntimeException('AmoCRM не вернул ID созданной сделки.');
        }
        $amoLeadUrl = buildAmoLeadUrl($amoLeadId);
        $action = 'created';
        coordinator_log_info('Создана новая сделка amo ОС', [
            'amo_lead_id' => $amoLeadId
        ]);
    }

    updateHollyAmoOsField($student, $studentData, $amoLeadUrl);

    echo json_encode([
        'success' => true,
        'action' => $action,
        'amo_lead_id' => $amoLeadId,
        'amo_lead_url' => $amoLeadUrl,
        'student_client_id' => $studentData['studentClientId'],
        'responsible' => $studentData['responsible'],
        'pipeline_id' => $routing['pipeline_id'],
        'status_id' => $routing['status_id']
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
} catch (Throwable $e) {
    coordinator_log_error('Ошибка обработки webhook координаторов', [
        'error' => $e->getMessage(),
        'trace' => $e->getTraceAsString()
    ]);

    http_response_code(500);
    echo json_encode([
        'success' => false,
        'error' => $e->getMessage()
    ], JSON_UNESCAPED_UNICODE);
}

function getCoordinatorRequestPayload(): array
{
    $rawInput = file_get_contents('php://input');
    $decoded = json_decode((string) $rawInput, true);
    if (is_array($decoded)) {
        return $decoded;
    }

    if (!empty($_POST) && is_array($_POST)) {
        return $_POST;
    }

    throw new RuntimeException('Не удалось декодировать входящий payload.');
}

function fetchHollyStudentForCoordinator(array $payload): array
{
    $studentClientId = extractStudentClientId($payload);
    if ($studentClientId <= 0) {
        throw new RuntimeException('В webhook не найден studentClientId/clientId.');
    }

    $response = callHollyhopApiForCoordinators('GetStudents', [
        'clientId' => $studentClientId
    ]);

    $students = [];
    if (isset($response['Students']) && is_array($response['Students'])) {
        $students = $response['Students'];
    } elseif (isset($response[0]) && is_array($response[0])) {
        $students = $response;
    } elseif (isset($response['ClientId']) || isset($response['clientId'])) {
        $students = [$response];
    }

    foreach ($students as $student) {
        $candidateId = (int) ($student['ClientId'] ?? $student['clientId'] ?? 0);
        if ($candidateId === $studentClientId) {
            return $student;
        }
    }

    throw new RuntimeException('Не удалось получить карточку студента из Hollyhop по clientId=' . $studentClientId);
}

function extractStudentClientId(array $payload): int
{
    $candidates = [
        $payload['studentClientId'] ?? null,
        $payload['clientId'] ?? null,
        $payload['ClientId'] ?? null,
        $payload['student']['clientId'] ?? null,
        $payload['student']['ClientId'] ?? null,
        $payload['data']['studentClientId'] ?? null,
        $payload['data']['clientId'] ?? null
    ];

    foreach ($candidates as $candidate) {
        if (is_numeric($candidate) && (int) $candidate > 0) {
            return (int) $candidate;
        }
    }

    return 0;
}

function callHollyhopApiForCoordinators(string $functionName, array $params): array
{
    $apiConfig = get_config('api');
    $url = rtrim((string) $apiConfig['base_url'], '/') . '/' . ltrim($functionName, '/');
    $params['authkey'] = (string) $apiConfig['auth_key'];
    $json = json_encode($params, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

    if ($json === false) {
        throw new RuntimeException('Не удалось сериализовать запрос Hollyhop.');
    }

    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => $url,
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => $json,
        CURLOPT_HTTPHEADER => [
            'Content-Type: application/json',
            'Content-Length: ' . strlen($json)
        ],
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT => 60,
        CURLOPT_CONNECTTIMEOUT => 15,
        CURLOPT_SSL_VERIFYPEER => true
    ]);

    $response = curl_exec($ch);
    $httpCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlError = curl_error($ch);
    curl_close($ch);

    if ($curlError !== '') {
        throw new RuntimeException('Ошибка cURL Hollyhop: ' . $curlError);
    }

    if ($httpCode >= 400) {
        throw new RuntimeException('Hollyhop API вернул HTTP ' . $httpCode . ': ' . (string) $response);
    }

    $decoded = json_decode((string) $response, true);
    if (!is_array($decoded)) {
        throw new RuntimeException('Некорректный ответ Hollyhop API: ' . mb_substr((string) $response, 0, 500, 'UTF-8'));
    }

    return $decoded;
}

function normalizeCoordinatorStudent(array $student, array $payload): array
{
    $studentClientId = (int) ($student['ClientId'] ?? $student['clientId'] ?? extractStudentClientId($payload));
    $profileId = (int) ($student['Id'] ?? $student['id'] ?? 0);
    $fullName = trim((string) (
        $student['Name'] ??
        $student['name'] ??
        buildFullNameFromParts(
            $student['LastName'] ?? $student['lastName'] ?? '',
            $student['FirstName'] ?? $student['firstName'] ?? '',
            $student['MiddleName'] ?? $student['middleName'] ?? ''
        )
    ));

    $studentPhone = firstNonEmptyValue([
        $student['Phone'] ?? null,
        $student['phone'] ?? null,
        $student['Mobile'] ?? null,
        $student['mobile'] ?? null
    ]);
    $studentEmail = firstNonEmptyValue([
        $student['EMail'] ?? null,
        $student['eMail'] ?? null,
        $student['Email'] ?? null,
        $student['email'] ?? null
    ]);

    $extraFields = $student['ExtraFields'] ?? [];
    $agents = is_array($student['Agents'] ?? null) ? $student['Agents'] : [];

    // "Ответственный" в Hollyhop.
    $responsible = trim((string) getHollyFieldValue(
        $student,
        [HOLLY_FIELD_RESPONSIBLE > 0 ? HOLLY_FIELD_RESPONSIBLE : null],
        ['Ответственный']
    ));

    // Основные учебные поля для сделки amoCRM.
    $discipline = trim((string) firstNonEmptyValue([
        $student['Discipline'] ?? null,
        $student['discipline'] ?? null,
        getHollyFieldValue($student, [], ['Язык'])
    ]));
    $level = trim((string) firstNonEmptyValue([
        $student['Level'] ?? null,
        $student['level'] ?? null,
        getHollyFieldValue($student, [], ['Уровень'])
    ]));
    $learningType = trim((string) firstNonEmptyValue([
        $student['LearningType'] ?? null,
        $student['learningType'] ?? null,
        getHollyFieldValue($student, [], ['Тип обучения'])
    ]));
    $office = trim((string) firstNonEmptyValue([
        $student['OfficeOrCompany'] ?? null,
        $student['officeOrCompany'] ?? null,
        $student['OfficeOrCompanyName'] ?? null,
        getHollyFieldValue($student, [], ['Филиал', 'Отделение'])
    ]));
    $birthDate = normalizeDateValue((string) firstNonEmptyValue([
        $student['Birthday'] ?? null,
        $student['birthday'] ?? null,
        getHollyFieldValue($student, [], ['Дата рождения', 'Дата рождения клиента'])
    ]));

    // Пользовательские поля Hollyhop из ТЗ.
    $hollyClub = trim((string) getHollyFieldValue($student, [], ['Языковой клуб']));
    $hollyClubUntil = normalizeDateValue((string) getHollyFieldValue($student, [], ['Дата окончания действия языкового клуба']));
    $comboPlatform = trim((string) getHollyFieldValue($student, [], ['Комбо платформа (ОС)']));
    // В этом поле Hollyhop храним ссылку на сделку amo ОС, чтобы обновлять её, а не плодить дубли.
    $amoOsLink = trim((string) getHollyFieldValue(
        $student,
        [HOLLY_FIELD_AMO_OS > 0 ? HOLLY_FIELD_AMO_OS : null],
        ['АМО (ОС)', 'АМО(ОС)']
    ));

    // Дополнительный контакт: плательщик.
    $payerName = trim((string) firstNonEmptyValue([
        getHollyFieldValue($student, [], ['Фио плательщика', 'ФИО плательщика']),
        getAgentValueByWhoIs($agents, ['плательщик', 'родитель'], 'name')
    ]));
    $payerRelation = trim((string) firstNonEmptyValue([
        getHollyFieldValue($student, [], ['Кем является студенту']),
        getAgentValueByWhoIs($agents, ['плательщик', 'родитель'], 'whoIs')
    ]));
    $payerPhone = normalizePhone((string) firstNonEmptyValue([
        getHollyFieldValue($student, [], ['Телефон плательщика', 'Телефон клиента']),
        getAgentValueByWhoIs($agents, ['плательщик', 'родитель'], 'phone')
    ]));
    $payerEmail = trim((string) firstNonEmptyValue([
        getHollyFieldValue($student, [], ['Почта плательщика', 'E-Mail клиента', 'Email клиента']),
        getAgentValueByWhoIs($agents, ['плательщик', 'родитель'], 'email')
    ]));

    return [
        'studentClientId' => $studentClientId,
        'profileId' => $profileId,
        'fullName' => $fullName !== '' ? $fullName : 'Студент ' . $studentClientId,
        'studentPhone' => normalizePhone((string) $studentPhone),
        'studentEmail' => trim((string) $studentEmail),
        'responsible' => $responsible,
        'discipline' => $discipline,
        'level' => $level,
        'learningType' => mapLearningTypeForAmo($learningType),
        'office' => mapOfficeForAmo($office),
        'birthDate' => $birthDate,
        'hollyClub' => normalizeBooleanLikeValue($hollyClub),
        'hollyClubUntil' => $hollyClubUntil,
        'comboPlatform' => $comboPlatform,
        'hollyProfileUrl' => buildHollyProfileUrl($profileId, $studentClientId),
        'amoOsLink' => $amoOsLink,
        'payerName' => $payerName,
        'payerRelation' => $payerRelation,
        'payerPhone' => $payerPhone,
        'payerEmail' => $payerEmail,
        'studentSnapshot' => $student,
        'extraFields' => is_array($extraFields) ? $extraFields : []
    ];
}

function resolveCoordinatorRouting(string $responsible): array
{
    $normalizedResponsible = mb_strtolower(trim($responsible), 'UTF-8');
    $map = [
        'алина' => [
            'label' => 'Алина',
            'amo_field_value' => AMO_COORD_ALINA_VALUE,
            'responsible_user_id' => AMO_COORD_ALINA_USER_ID,
            'pipeline_id' => AMO_COORD_ALINA_PIPELINE_ID,
            'status_id' => AMO_COORD_ALINA_STATUS_ID
        ],
        'ирина' => [
            'label' => 'Ирина',
            'amo_field_value' => AMO_COORD_IRINA_VALUE,
            'responsible_user_id' => AMO_COORD_IRINA_USER_ID,
            'pipeline_id' => AMO_COORD_IRINA_PIPELINE_ID,
            'status_id' => AMO_COORD_IRINA_STATUS_ID
        ],
        'шабо' => [
            'label' => 'Шабо',
            'amo_field_value' => AMO_COORD_SHABO_VALUE,
            'responsible_user_id' => AMO_COORD_SHABO_USER_ID,
            'pipeline_id' => AMO_COORD_SHABO_PIPELINE_ID,
            'status_id' => AMO_COORD_SHABO_STATUS_ID
        ]
    ];

    $routing = $map[$normalizedResponsible] ?? [
        'label' => $responsible !== '' ? $responsible : 'Общая',
        'amo_field_value' => $responsible,
        'responsible_user_id' => 0,
        'pipeline_id' => AMO_COORD_GENERAL_PIPELINE_ID,
        'status_id' => AMO_COORD_GENERAL_STATUS_ID
    ];

    if ($routing['pipeline_id'] <= 0 || $routing['status_id'] <= 0) {
        throw new RuntimeException('Не настроены pipeline_id/status_id для координаторской логики. Заполните константы в amo_coordinators_hook.php.');
    }

    return $routing;
}

function createCoordinatorLead(array $studentData, array $routing): array
{
    $lead = buildCoordinatorLeadPayload($studentData, $routing);
    $contacts = buildCoordinatorContactsPayload($studentData);
    if ($contacts !== []) {
        $lead['_embedded'] = ['contacts' => $contacts];
    }

    $response = post_or_patch($GLOBALS['subdomain'], [$lead], '/api/v4/leads/complex', $GLOBALS['data'], 'POST');
    if (!is_array($response) || !isset($response[0]) || !is_array($response[0])) {
        throw new RuntimeException('Некорректный ответ AmoCRM при создании lead/contacts.');
    }

    return $response[0];
}

function updateCoordinatorLead(int $leadId, array $studentData, array $routing): void
{
    $lead = buildCoordinatorLeadPayload($studentData, $routing);
    $lead['id'] = $leadId;
    post_or_patch($GLOBALS['subdomain'], [$lead], '/api/v4/leads', $GLOBALS['data'], 'PATCH');
}

function buildCoordinatorLeadPayload(array $studentData, array $routing): array
{
    $customFields = [];

    // Маппинг полей Hollyhop -> сделка amoCRM.
    appendAmoFieldValue($customFields, AMO_FIELD_DISCIPLINE, $studentData['discipline']);
    appendAmoFieldValue($customFields, AMO_FIELD_LEVEL, $studentData['level']);
    appendAmoFieldValue($customFields, AMO_FIELD_LEARNING_FORMAT, $studentData['learningType']);
    appendAmoFieldValue($customFields, AMO_FIELD_OFFICE, $studentData['office']);
    appendAmoFieldValue($customFields, AMO_FIELD_RESPONSIBLE, $routing['amo_field_value']);
    appendAmoFieldValue($customFields, AMO_FIELD_HOLLY_LINK, $studentData['hollyProfileUrl']);
    appendAmoFieldValue($customFields, AMO_FIELD_BIRTHDATE, $studentData['birthDate']);
    appendAmoFieldValue($customFields, AMO_FIELD_LANGUAGE_CLUB, $studentData['hollyClub']);
    appendAmoFieldValue($customFields, AMO_FIELD_LANGUAGE_CLUB_UNTIL, $studentData['hollyClubUntil']);
    appendAmoFieldValue($customFields, AMO_FIELD_COMBO_PLATFORM_OS, $studentData['comboPlatform']);

    $lead = [
        'name' => $studentData['fullName'],
        'pipeline_id' => $routing['pipeline_id'],
        'status_id' => $routing['status_id'],
        'custom_fields_values' => $customFields
    ];

    if ($routing['responsible_user_id'] > 0) {
        $lead['responsible_user_id'] = $routing['responsible_user_id'];
    }

    return $lead;
}

function buildCoordinatorContactsPayload(array $studentData): array
{
    $contacts = [];

    // Основной контакт: сам студент.
    $studentContactFields = [];
    appendContactFieldValue($studentContactFields, AMO_CONTACT_FIELD_PHONE, $studentData['studentPhone'], 'WORK');
    appendContactFieldValue($studentContactFields, AMO_CONTACT_FIELD_EMAIL, $studentData['studentEmail'], 'WORK');
    $contacts[] = [
        'name' => $studentData['fullName'],
        'is_main' => true,
        'custom_fields_values' => $studentContactFields
    ];

    if ($studentData['payerName'] !== '' || $studentData['payerPhone'] !== '' || $studentData['payerEmail'] !== '') {
        // Дополнительный контакт: плательщик, в должность передаем "Кем является студенту".
        $payerContactFields = [];
        appendContactFieldValue($payerContactFields, AMO_CONTACT_FIELD_PHONE, $studentData['payerPhone'], 'WORK');
        appendContactFieldValue($payerContactFields, AMO_CONTACT_FIELD_EMAIL, $studentData['payerEmail'], 'WORK');
        appendContactFieldValue(
            $payerContactFields,
            AMO_CONTACT_FIELD_POSITION,
            $studentData['payerRelation']
        );

        $contacts[] = [
            'name' => $studentData['payerName'] !== '' ? $studentData['payerName'] : 'Плательщик',
            'custom_fields_values' => $payerContactFields
        ];
    }

    foreach ($contacts as &$contact) {
        if (empty($contact['custom_fields_values'])) {
            unset($contact['custom_fields_values']);
        }
    }
    unset($contact);

    return $contacts;
}

function updateHollyAmoOsField(array $student, array $studentData, string $amoLeadUrl): void
{
    if ($amoLeadUrl === '' || $studentData['studentClientId'] <= 0) {
        return;
    }

    // После создания/обновления сделки записываем ссылку обратно в Hollyhop, в поле "АМО (ОС)".
    $targetFieldId = HOLLY_FIELD_AMO_OS;
    $fieldName = 'АМО (ОС)';
    $fields = [];
    $replaced = false;

    foreach (($student['ExtraFields'] ?? []) as $field) {
        if (!is_array($field)) {
            continue;
        }

        $currentId = (int) ($field['Id'] ?? $field['id'] ?? 0);
        $currentName = trim((string) ($field['Name'] ?? $field['name'] ?? ''));
        $currentValue = $field['Value'] ?? $field['value'] ?? '';

        $isTarget = false;
        if ($targetFieldId > 0 && $currentId === $targetFieldId) {
            $isTarget = true;
        } elseif ($currentName !== '' && normalizeFieldName($currentName) === normalizeFieldName($fieldName)) {
            $isTarget = true;
        }

        if ($isTarget) {
            $fields[] = [
                'name' => $currentName !== '' ? $currentName : $fieldName,
                'value' => $amoLeadUrl
            ];
            $replaced = true;
            continue;
        }

        if ($currentName !== '') {
            $fields[] = [
                'name' => $currentName,
                'value' => $currentValue
            ];
        }
    }

    if (!$replaced) {
        $fields[] = [
            'name' => $fieldName,
            'value' => $amoLeadUrl
        ];
    }

    callHollyhopApiForCoordinators('EditUserExtraFields', [
        'studentClientId' => $studentData['studentClientId'],
        'fields' => $fields
    ]);
}

function appendAmoFieldValue(array &$customFields, int $fieldId, string $value, ?string $enumCode = null): void
{
    $value = trim($value);
    if ($fieldId <= 0 || $value === '') {
        return;
    }

    $field = [
        'field_id' => $fieldId,
        'values' => [[
            'value' => $value
        ]]
    ];
    if ($enumCode !== null && $enumCode !== '') {
        $field['values'][0]['enum_code'] = $enumCode;
    }

    $customFields[] = $field;
}

function appendContactFieldValue(array &$customFields, int $fieldId, string $value, ?string $enumCode = null): void
{
    appendAmoFieldValue($customFields, $fieldId, $value, $enumCode);
}

function getHollyFieldValue(array $student, array $fieldIds, array $fieldNames): ?string
{
    $directFields = [
        'ExtraFields',
        'extraFields'
    ];

    foreach ($directFields as $extraFieldKey) {
        if (!isset($student[$extraFieldKey]) || !is_array($student[$extraFieldKey])) {
            continue;
        }

        foreach ($student[$extraFieldKey] as $field) {
            if (!is_array($field)) {
                continue;
            }

            $currentId = (string) ($field['Id'] ?? $field['id'] ?? '');
            $currentName = trim((string) ($field['Name'] ?? $field['name'] ?? ''));
            $currentValue = $field['Value'] ?? $field['value'] ?? null;

            foreach ($fieldIds as $fieldId) {
                if ($fieldId !== null && (string) $fieldId !== '' && $currentId === (string) $fieldId) {
                    return is_scalar($currentValue) ? trim((string) $currentValue) : null;
                }
            }

            foreach ($fieldNames as $fieldName) {
                if ($currentName !== '' && normalizeFieldName($currentName) === normalizeFieldName($fieldName)) {
                    return is_scalar($currentValue) ? trim((string) $currentValue) : null;
                }
            }
        }
    }

    return null;
}

function getAgentValueByWhoIs(array $agents, array $whoIsOptions, string $valueType): ?string
{
    foreach ($agents as $agent) {
        if (!is_array($agent)) {
            continue;
        }

        $whoIs = mb_strtolower(trim((string) ($agent['WhoIs'] ?? $agent['whoIs'] ?? '')), 'UTF-8');
        if ($whoIs === '' || !in_array($whoIs, $whoIsOptions, true)) {
            continue;
        }

        if ($valueType === 'name') {
            return buildFullNameFromParts(
                $agent['LastName'] ?? $agent['lastName'] ?? '',
                $agent['FirstName'] ?? $agent['firstName'] ?? '',
                $agent['MiddleName'] ?? $agent['middleName'] ?? ''
            );
        }

        if ($valueType === 'phone') {
            return firstNonEmptyValue([
                $agent['Mobile'] ?? null,
                $agent['mobile'] ?? null,
                $agent['Phone'] ?? null,
                $agent['phone'] ?? null
            ]);
        }

        if ($valueType === 'email') {
            return firstNonEmptyValue([
                $agent['EMail'] ?? null,
                $agent['eMail'] ?? null,
                $agent['Email'] ?? null,
                $agent['email'] ?? null
            ]);
        }

        if ($valueType === 'whoIs') {
            return trim((string) ($agent['WhoIs'] ?? $agent['whoIs'] ?? ''));
        }
    }

    return null;
}

function mapLearningTypeForAmo(string $learningType): string
{
    $normalized = mb_strtolower(trim($learningType), 'UTF-8');
    if ($normalized === '') {
        return '';
    }

    if (in_array($normalized, ['мини', 'мини-группа', 'мини группа', 'группа'], true)) {
        return 'В группе';
    }
    if (in_array($normalized, ['индивидуально', 'индивидуальный', 'индивидуальные'], true)) {
        return 'Индивидуально';
    }
    if (in_array($normalized, ['самостоятельно на платформе', 'самостоятельно. платформа', 'самостоятельно'], true)) {
        return 'Самостоятельное обучение';
    }

    return 'Индивидуально';
}

function mapOfficeForAmo(string $office): string
{
    $normalized = mb_strtolower(trim($office), 'UTF-8');
    if ($normalized === '') {
        return '';
    }

    if (in_array($normalized, ['онлайн платформа', 'онлайн zoom', 'телемост', 'онлайн телемост'], true)) {
        return 'Онлайн';
    }
    if ($normalized === 'курская') {
        return 'Курская';
    }
    if ($normalized === 'красная пресня') {
        return 'Красная Пресня';
    }
    if ($normalized === 'октябрьская') {
        return 'Октябрьская';
    }
    if ($normalized === 'выезд') {
        return 'Выезд';
    }
    if ($normalized === 'самостоятельно. платформа') {
        return 'Самостоятельно. Платформа';
    }

    return 'Корпор клиент';
}

function buildHollyProfileUrl(int $profileId, int $studentClientId): string
{
    $subdomain = trim((string) get_config('api.subdomain'));
    if ($subdomain === '') {
        return '';
    }
    if ($profileId > 0) {
        return 'https://' . $subdomain . '.t8s.ru/Profile/' . $profileId;
    }

    return 'https://' . $subdomain . '.t8s.ru';
}

function buildAmoLeadUrl(int $leadId): string
{
    return 'https://' . $GLOBALS['subdomain'] . '.amocrm.ru/leads/detail/' . $leadId;
}

function parseAmoLeadIdFromString(string $value): int
{
    if ($value === '') {
        return 0;
    }

    if (preg_match('~/leads/detail/(\d+)~', $value, $matches)) {
        return (int) $matches[1];
    }
    if (preg_match('~\b(\d{5,})\b~', $value, $matches)) {
        return (int) $matches[1];
    }

    return 0;
}

function buildFullNameFromParts($lastName, $firstName, $middleName): string
{
    $parts = array_filter([
        trim((string) $lastName),
        trim((string) $firstName),
        trim((string) $middleName)
    ], static fn (string $value): bool => $value !== '');

    return implode(' ', $parts);
}

function normalizeFieldName(string $value): string
{
    $value = str_replace('ё', 'е', mb_strtolower(trim($value), 'UTF-8'));
    return preg_replace('/\s+/u', ' ', $value) ?? $value;
}

function normalizeDateValue(string $value): string
{
    $value = trim($value);
    if ($value === '') {
        return '';
    }

    $value = str_replace('/', '.', $value);
    if (preg_match('/^\d{4}-\d{2}-\d{2}$/', $value)) {
        return $value;
    }
    if (preg_match('/^(\d{2})\.(\d{2})\.(\d{4})$/', $value, $matches)) {
        return $matches[3] . '-' . $matches[2] . '-' . $matches[1];
    }

    $timestamp = strtotime($value);
    return $timestamp ? date('Y-m-d', $timestamp) : $value;
}

function normalizePhone(string $value): string
{
    $digits = preg_replace('/\D+/', '', $value) ?? '';
    if ($digits === '') {
        return '';
    }
    if (strlen($digits) === 11 && $digits[0] === '8') {
        $digits = '7' . substr($digits, 1);
    }

    return $digits;
}

function normalizeBooleanLikeValue(string $value): string
{
    $normalized = mb_strtolower(trim($value), 'UTF-8');
    if ($normalized === '') {
        return '';
    }
    if (in_array($normalized, ['да', 'yes', 'true', '1'], true)) {
        return 'Да';
    }
    if (in_array($normalized, ['нет', 'no', 'false', '0'], true)) {
        return 'Нет';
    }

    return $value;
}

function firstNonEmptyValue(array $values): ?string
{
    foreach ($values as $value) {
        if (is_scalar($value) && trim((string) $value) !== '') {
            return trim((string) $value);
        }
    }

    return null;
}

function maskCoordinatorStudentData(array $studentData): array
{
    $masked = $studentData;
    foreach (['studentPhone', 'payerPhone'] as $field) {
        if (!empty($masked[$field])) {
            $masked[$field] = substr($masked[$field], 0, 2) . str_repeat('*', max(0, strlen($masked[$field]) - 4)) . substr($masked[$field], -2);
        }
    }
    foreach (['studentEmail', 'payerEmail'] as $field) {
        if (!empty($masked[$field])) {
            $masked[$field] = preg_replace('/(^.).*(@.*$)/', '$1***$2', $masked[$field]) ?? $masked[$field];
        }
    }
    unset($masked['studentSnapshot'], $masked['extraFields']);

    return $masked;
}

function coordinator_log(string $level, string $message, $data = null): void
{
    $logDir = __DIR__ . '/logs';
    $logFile = $logDir . '/coordinators.log';

    if (!is_dir($logDir)) {
        @mkdir($logDir, 0755, true);
    }

    $timestamp = date('Y-m-d H:i:s');
    $entry = "[{$timestamp}] [{$level}] [amo_coordinators_hook.php] {$message}";

    if ($data !== null) {
        if (is_string($data)) {
            $entry .= "\n" . $data;
        } else {
            $entry .= "\n" . json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        }
    }

    $entry .= "\n" . str_repeat('-', 80) . "\n";
    @file_put_contents($logFile, $entry, FILE_APPEND | LOCK_EX);
}

function coordinator_log_info(string $message, $data = null): void
{
    coordinator_log('INFO', $message, $data);
}

function coordinator_log_error(string $message, $data = null): void
{
    coordinator_log('ERROR', $message, $data);
}
