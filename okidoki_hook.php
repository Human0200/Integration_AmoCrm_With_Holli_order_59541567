<?php

declare(strict_types=1);

require_once __DIR__ . '/config.php';
require_once __DIR__ . '/logger.php';

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

$rawInput = file_get_contents('php://input');
$payload = json_decode($rawInput, true);
$payloadHash = hash('sha256', (string) $rawInput);

if (!is_array($payload)) {
    okidoki_log_warning('OkiDoki: не удалось декодировать JSON', [
        'payload_hash' => $payloadHash,
        'raw_input_preview' => mb_substr((string) $rawInput, 0, 1000, 'UTF-8')
    ]);

    http_response_code(400);
    echo json_encode([
        'success' => false,
        'error' => 'Некорректный JSON'
    ], JSON_UNESCAPED_UNICODE);
    exit;
}

okidoki_log_info('OkiDoki: callback получен', buildOkiPayloadDebugContext($payload, $payloadHash));

$statusInfo = getOkiStatusInfo($payload);

if (!isOkiDokiSignedContract($payload)) {
    okidoki_log_info('OkiDoki: вебхук пропущен, статус не signed', [
        'payload_hash' => $payloadHash,
        'status' => $statusInfo
    ]);

    echo json_encode([
        'success' => true,
        'ignored' => true,
        'reason' => 'status is not signed',
        'status' => $statusInfo['normalized'] ?? null
    ], JSON_UNESCAPED_UNICODE);
    exit;
}

try {
    $studentPayload = buildStudentPayloadFromOkiDoki($payload);

    okidoki_log_info('OkiDoki: подготовлены данные для add_student.php', [
        'payload_hash' => $payloadHash,
        'student_payload' => maskSensitiveData($studentPayload)
    ]);

    $addStudentResponse = sendPayloadToAddStudent($studentPayload);

    okidoki_log_info('OkiDoki: ответ от add_student.php получен', [
        'payload_hash' => $payloadHash,
        'response' => maskSensitiveData($addStudentResponse)
    ]);

    echo json_encode([
        'success' => true,
        'source' => 'okidoki',
        'status' => $statusInfo['normalized'] ?? null,
        'hollyhop_response' => $addStudentResponse
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
} catch (Throwable $e) {
    okidoki_log_error('OkiDoki: ошибка обработки webhook', [
        'payload_hash' => $payloadHash,
        'error' => $e->getMessage(),
        'trace' => $e->getTraceAsString()
    ]);

    http_response_code(500);
    echo json_encode([
        'success' => false,
        'error' => $e->getMessage()
    ], JSON_UNESCAPED_UNICODE);
}

function isOkiDokiSignedContract(array $data): bool
{
    return getOkiStatusInfo($data)['is_signed'];
}

function okidoki_log(string $level, string $message, $data = null): void
{
    $logDir = __DIR__ . '/logs';
    $logFile = $logDir . '/okidoki.log';

    if (!is_dir($logDir)) {
        @mkdir($logDir, 0755, true);
    }

    $timestamp = date('Y-m-d H:i:s');
    $entry = "[{$timestamp}] [{$level}] [okidoki_hook.php] {$message}";

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

function okidoki_log_info(string $message, $data = null): void
{
    okidoki_log('INFO', $message, $data);
}

function okidoki_log_warning(string $message, $data = null): void
{
    okidoki_log('WARNING', $message, $data);
}

function okidoki_log_error(string $message, $data = null): void
{
    okidoki_log('ERROR', $message, $data);
}

function buildStudentPayloadFromOkiDoki(array $payload): array
{
    $parentName = trim((string) getOkiField($payload, ['ФИО клиента']));
    if ($parentName === '') {
        $parentName = buildOkiClientFullName($payload);
    }

    $email = trim((string) getOkiField($payload, ['E-Mail клиента', 'Email клиента']));
    $phone = normalizeOkiPhone((string) getOkiField($payload, ['Телефон', 'Телефон клиента', 'Телефон заказчика', 'Мобильный телефон']));
    $emergencyPhone = normalizeOkiPhone((string) getOkiField($payload, ['Телефон (экстренный)', 'Экстренный телефон', 'Дополнительный телефон']));
    $childName = trim((string) getOkiField($payload, ['ФИО ребенка', 'ФИО ребёнка']));
    $childBirthDate = normalizeOkiDate((string) getOkiField($payload, ['Дата рождения ребенка', 'Дата рождения ребёнка']));
    $language = normalizeOkiLanguage((string) getOkiField($payload, ['Язык', 'Иностранный язык', 'Язык ребенка', 'Язык ребёнка']));
    $level = normalizeOkiLevel((string) getOkiField($payload, ['Уровень языка', 'Уровень владения языком']));

    $nameParts = splitFullName($childName !== '' ? $childName : $parentName);

    $studentPayload = [
        'firstName' => $nameParts['firstName'] !== '' ? $nameParts['firstName'] : '-',
        'lastName' => $nameParts['lastName'] !== '' ? $nameParts['lastName'] : '-',
        'gender' => 'F',
        'Status' => 'В наборе'
    ];

    if ($nameParts['middleName'] !== '') {
        $studentPayload['middleName'] = $nameParts['middleName'];
    }
    if ($email !== '') {
        $studentPayload['email'] = $email;
        $studentPayload['parentEmail'] = $email;
    }
    if ($phone !== '') {
        $studentPayload['parentPhone'] = $phone;
    }
    if ($emergencyPhone !== '') {
        $studentPayload['parentEmergencyPhone'] = $emergencyPhone;
    }
    if ($parentName !== '') {
        $studentPayload['parentName'] = $parentName;
    }
    if ($childName !== '') {
        $studentPayload['childName'] = $childName;
    }
    if ($childBirthDate !== '') {
        $studentPayload['birthDate'] = $childBirthDate;
        $studentPayload['childBirthDate'] = $childBirthDate;
        $studentPayload['Дата рождения'] = $childBirthDate;
    }
    if ($language !== '') {
        $studentPayload['discipline'] = $language;
    }
    if ($level !== '') {
        $studentPayload['level'] = $level;
    }
    $studentPayload['sourceSystem'] = 'okidoki';
    if (!empty($payload['lead_id'])) {
        $studentPayload['amo_lead_id'] = (int) $payload['lead_id'];
    }

    return $studentPayload;
}

function sendPayloadToAddStudent(array $studentPayload): array
{
    $url = buildLocalEndpointUrl('add_student.php');
    $json = json_encode($studentPayload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

    if ($json === false) {
        throw new RuntimeException('Не удалось сериализовать payload для add_student.php');
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
        CURLOPT_TIMEOUT => 120,
        CURLOPT_CONNECTTIMEOUT => 15,
        CURLOPT_SSL_VERIFYPEER => true,
    ]);

    $response = curl_exec($ch);
    $httpCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlError = curl_error($ch);
    curl_close($ch);

    if ($curlError !== '') {
        throw new RuntimeException('Ошибка cURL при вызове add_student.php: ' . $curlError);
    }

    if ($httpCode >= 400) {
        throw new RuntimeException('add_student.php вернул HTTP ' . $httpCode . ': ' . (string) $response);
    }

    $decoded = json_decode(trim((string) $response), true);
    if (!is_array($decoded)) {
        throw new RuntimeException('Некорректный ответ add_student.php: ' . mb_substr((string) $response, 0, 500, 'UTF-8'));
    }

    return $decoded;
}

function buildLocalEndpointUrl(string $scriptName): string
{
    $https = $_SERVER['HTTPS'] ?? '';
    $isHttps = $https !== '' && strtolower((string) $https) !== 'off';
    $scheme = $isHttps ? 'https' : 'http';
    $host = $_SERVER['HTTP_HOST'] ?? '';

    if ($host === '') {
        throw new RuntimeException('Не удалось определить HTTP_HOST для вызова add_student.php');
    }

    $scriptDir = rtrim(str_replace('\\', '/', dirname($_SERVER['SCRIPT_NAME'] ?? '/')), '/');
    $path = ($scriptDir !== '' ? $scriptDir : '') . '/' . ltrim($scriptName, '/');

    return $scheme . '://' . $host . $path;
}

function getOkiField(array $payload, array $keys): ?string
{
    $sources = [];

    $extraFields = $payload['extra_fields'] ?? [];
    if (is_array($extraFields)) {
        $sources[] = $extraFields;
    }

    $entitiesMap = buildOkiKeywordMap($payload['entities'] ?? null);
    if (!empty($entitiesMap)) {
        $sources[] = $entitiesMap;
    }

    $systemEntitiesMap = buildOkiKeywordMap($payload['system_entities'] ?? null);
    if (!empty($systemEntitiesMap)) {
        $sources[] = $systemEntitiesMap;
    }

    foreach ($sources as $source) {
        $normalizedSource = buildNormalizedOkiFieldMap($source);

        foreach ($keys as $key) {
            $value = $source[$key] ?? null;
            if (is_scalar($value) && trim((string) $value) !== '') {
                return trim((string) $value);
            }

            $normalizedValue = $normalizedSource[normalizeOkiLookupKey($key)] ?? null;
            if (is_string($normalizedValue) && trim($normalizedValue) !== '') {
                return trim($normalizedValue);
            }
        }
    }

    return null;
}

function buildNormalizedOkiFieldMap(array $source): array
{
    $normalized = [];

    foreach ($source as $key => $value) {
        if (!is_scalar($value)) {
            continue;
        }

        $normalized[normalizeOkiLookupKey((string) $key)] = trim((string) $value);
    }

    return $normalized;
}

function normalizeOkiLookupKey(string $value): string
{
    $value = trim($value);
    $value = mb_strtolower($value, 'UTF-8');
    $value = str_replace('ё', 'е', $value);
    $value = preg_replace('/\s+/u', ' ', $value);

    return trim((string) $value);
}

function buildOkiKeywordMap($items): array
{
    if (!is_array($items)) {
        return [];
    }

    $map = [];

    foreach ($items as $item) {
        if (!is_array($item)) {
            continue;
        }

        $keyword = isset($item['keyword']) && is_scalar($item['keyword'])
            ? trim((string) $item['keyword'])
            : '';

        if ($keyword === '') {
            continue;
        }

        $value = $item['value'] ?? null;
        if (!is_scalar($value)) {
            continue;
        }

        $map[$keyword] = trim((string) $value);
    }

    return $map;
}

function buildOkiClientFullName(array $payload): string
{
    $parts = [
        trim((string) getOkiField($payload, ['Фамилия клиента'])),
        trim((string) getOkiField($payload, ['Имя клиента'])),
        trim((string) getOkiField($payload, ['Отчество клиента']))
    ];

    $parts = array_values(array_filter($parts, static function ($part) {
        return $part !== '';
    }));

    return implode(' ', $parts);
}

function getOkiStatusInfo(array $payload): array
{
    $status = $payload['status'] ?? null;
    $rawValue = null;
    $name = null;
    $internalId = null;

    if (is_string($status)) {
        $rawValue = trim($status);
        $name = $rawValue;
    } elseif (is_array($status)) {
        if (isset($status['name']) && is_scalar($status['name'])) {
            $name = trim((string) $status['name']);
        }
        if (isset($status['internal_id']) && is_scalar($status['internal_id'])) {
            $internalId = (string) $status['internal_id'];
        }
        $rawValue = $name;
    }

    $normalized = mb_strtolower((string) $rawValue, 'UTF-8');
    $normalized = str_replace(['ё', '_', '-'], ['е', ' ', ' '], $normalized);
    $normalized = preg_replace('/\s+/u', ' ', $normalized);
    $normalized = trim((string) $normalized);

    $signedStatuses = [
        'signed',
        'подписан',
        'подписано',
        'подписан клиентом',
        'договор подписан'
    ];

    return [
        'raw' => $status,
        'name' => $name,
        'internal_id' => $internalId,
        'normalized' => $normalized,
        'is_signed' => in_array($normalized, $signedStatuses, true)
    ];
}

function buildOkiPayloadDebugContext(array $payload, string $payloadHash): array
{
    $extraFields = $payload['extra_fields'] ?? [];
    $extraFieldKeys = is_array($extraFields) ? array_keys($extraFields) : [];

    return [
        'payload_hash' => $payloadHash,
        'top_level_keys' => array_keys($payload),
        'status' => getOkiStatusInfo($payload),
        'lead_id' => $payload['lead_id'] ?? null,
        'extra_fields_count' => count($extraFieldKeys),
        'extra_fields_keys' => $extraFieldKeys,
        'payload_masked' => maskSensitiveData($payload)
    ];
}

function maskSensitiveData($value)
{
    if (is_array($value)) {
        $masked = [];
        foreach ($value as $key => $item) {
            $masked[$key] = maskSensitiveValueByKey((string) $key, $item);
        }

        return $masked;
    }

    return $value;
}

function maskSensitiveValueByKey(string $key, $value)
{
    if (is_array($value)) {
        return maskSensitiveData($value);
    }

    if (!is_scalar($value) && $value !== null) {
        return $value;
    }

    $stringValue = trim((string) $value);
    if ($stringValue === '') {
        return $value;
    }

    $normalizedKey = mb_strtolower($key, 'UTF-8');
    $normalizedKey = str_replace('ё', 'е', $normalizedKey);

    if (strpos($normalizedKey, 'mail') !== false || strpos($normalizedKey, 'email') !== false) {
        return maskEmail($stringValue);
    }

    if (strpos($normalizedKey, 'телефон') !== false || strpos($normalizedKey, 'phone') !== false || strpos($normalizedKey, 'mobile') !== false) {
        return maskPhone($stringValue);
    }

    if (strpos($normalizedKey, 'фио') !== false || strpos($normalizedKey, 'name') !== false) {
        return maskHumanName($stringValue);
    }

    return $value;
}

function maskEmail(string $value): string
{
    $parts = explode('@', $value, 2);
    if (count($parts) !== 2) {
        return '***';
    }

    $localPart = $parts[0];
    $domain = $parts[1];
    $visibleLocal = mb_substr($localPart, 0, 2, 'UTF-8');

    return $visibleLocal . '***@' . $domain;
}

function maskPhone(string $value): string
{
    $digits = preg_replace('/\D+/', '', $value);
    if ($digits === '') {
        return '***';
    }

    $prefix = substr($digits, 0, min(2, strlen($digits)));
    $suffix = substr($digits, -2);

    return $prefix . str_repeat('*', max(0, strlen($digits) - strlen($prefix) - strlen($suffix))) . $suffix;
}

function maskHumanName(string $value): string
{
    $parts = preg_split('/\s+/u', trim($value)) ?: [];
    $maskedParts = [];

    foreach ($parts as $part) {
        $firstChar = mb_substr($part, 0, 1, 'UTF-8');
        $maskedParts[] = $firstChar . '***';
    }

    return implode(' ', $maskedParts);
}

function splitFullName(string $fullName): array
{
    $fullName = trim($fullName);
    if ($fullName === '') {
        return ['firstName' => '', 'lastName' => '', 'middleName' => ''];
    }

    $parts = preg_split('/\s+/u', $fullName) ?: [];

    return [
        'firstName' => $parts[0] ?? '',
        'lastName' => $parts[1] ?? '',
        'middleName' => count($parts) > 2 ? implode(' ', array_slice($parts, 2)) : ''
    ];
}

function normalizeOkiLevel(string $value): string
{
    $value = trim($value);
    if ($value === '') {
        return '';
    }

    $normalized = mb_strtolower($value, 'UTF-8');
    $normalized = str_replace(['ё', '‑', '–', '—', '_'], ['е', '-', '-', '-', ' '], $normalized);
    $normalized = preg_replace('/\s+/u', ' ', $normalized);
    $normalized = str_replace(
        ['а', 'в', 'с'],
        ['a', 'b', 'c'],
        $normalized
    );

    if (preg_match('/\b([abc][0-2])\b/u', $normalized, $matches)) {
        return strtoupper($matches[1]);
    }

    $map = [
        'a1' => 'A1',
        'beginner' => 'A1',
        'a2' => 'A2',
        'elementary' => 'A2',
        'b1' => 'B1',
        'pre-intermediate' => 'B1',
        'pre intermediate' => 'B1',
    ];

    return $map[$normalized] ?? $value;
}

function normalizeOkiLanguage(string $value): string
{
    $value = trim($value);
    if ($value === '') {
        return '';
    }

    $normalized = normalizeOkiLookupKey($value);

    $map = [
        'японский' => 'Японский',
        'корейский' => 'Корейский',
        'арабский' => 'Арабский',
        'турецкий' => 'Турецкий',
        'испанский' => 'Испанский',
        'итальянский' => 'Итальянский',
        'персидский' => 'Персидский',
        'английский' => 'Английский',
        'english' => 'Английский',
        'хинди' => 'Хинди',
        'иврит' => 'Иврит',
        'рки' => 'РКИ',
        'французский' => 'Французский',
        'немецкий' => 'Немецкий',
        'китайский' => 'Китайский',
        'chinese' => 'Китайский',
        'korean' => 'Корейский',
        'japanese' => 'Японский',
        'turkish' => 'Турецкий',
        'arabic' => 'Арабский',
        'spanish' => 'Испанский',
        'german' => 'Немецкий',
        'french' => 'Французский',
        'italian' => 'Итальянский',
        'hindi' => 'Хинди',
        'persian' => 'Персидский',
        'hebrew' => 'Иврит',
        'rki' => 'РКИ',
    ];

    return $map[$normalized] ?? $value;
}

function normalizeOkiDate(string $value): string
{
    $value = trim($value);
    if ($value === '') {
        return '';
    }

    if (preg_match('/^\d{4}-\d{2}-\d{2}$/', $value)) {
        return $value;
    }

    if (preg_match('/^(\d{2})\.(\d{2})\.(\d{4})$/', $value, $matches)) {
        return $matches[3] . '-' . $matches[2] . '-' . $matches[1];
    }

    if (preg_match('/^(\d{2})\/(\d{2})\/(\d{4})$/', $value, $matches)) {
        return $matches[3] . '-' . $matches[2] . '-' . $matches[1];
    }

    $monthMap = [
        'января' => '01',
        'февраля' => '02',
        'марта' => '03',
        'апреля' => '04',
        'мая' => '05',
        'июня' => '06',
        'июля' => '07',
        'августа' => '08',
        'сентября' => '09',
        'октября' => '10',
        'ноября' => '11',
        'декабря' => '12',
    ];

    if (preg_match('/^(\d{1,2})\s+([[:alpha:]]+)\s+(\d{4})(?:\s*г\.?)?$/u', mb_strtolower($value, 'UTF-8'), $matches)) {
        $day = str_pad($matches[1], 2, '0', STR_PAD_LEFT);
        $monthName = $matches[2];
        $year = $matches[3];

        if (isset($monthMap[$monthName])) {
            return $year . '-' . $monthMap[$monthName] . '-' . $day;
        }
    }

    return $value;
}

function normalizeOkiPhone(string $value): string
{
    $value = trim($value);
    if ($value === '') {
        return '';
    }

    $digits = preg_replace('/\D+/', '', $value);
    if ($digits === '') {
        return '';
    }

    if (strlen($digits) === 11 && $digits[0] === '8') {
        $digits = '7' . substr($digits, 1);
    }

    return $digits;
}
