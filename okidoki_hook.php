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

if (!is_array($payload)) {
    okidoki_log_warning('OkiDoki: не удалось декодировать JSON', [
        'raw_input_preview' => mb_substr((string) $rawInput, 0, 1000, 'UTF-8')
    ]);

    http_response_code(400);
    echo json_encode([
        'success' => false,
        'error' => 'Некорректный JSON'
    ], JSON_UNESCAPED_UNICODE);
    exit;
}

if (!isOkiDokiSignedContract($payload)) {
    okidoki_log_info('OkiDoki: вебхук пропущен, статус не signed', [
        'status' => $payload['status'] ?? null
    ]);

    echo json_encode([
        'success' => true,
        'ignored' => true,
        'reason' => 'status is not signed'
    ], JSON_UNESCAPED_UNICODE);
    exit;
}

try {
    $studentPayload = buildStudentPayloadFromOkiDoki($payload);

    okidoki_log_info('OkiDoki: подготовлены данные для add_student.php', $studentPayload);

    $addStudentResponse = sendPayloadToAddStudent($studentPayload);

    okidoki_log_info('OkiDoki: ответ от add_student.php получен', $addStudentResponse);

    echo json_encode([
        'success' => true,
        'source' => 'okidoki',
        'student_payload' => $studentPayload,
        'hollyhop_response' => $addStudentResponse
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
} catch (Throwable $e) {
    okidoki_log_error('OkiDoki: ошибка обработки webhook', [
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
    return isset($data['status']) && $data['status'] === 'signed';
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
    $email = trim((string) getOkiField($payload, ['E-Mail клиента', 'Email клиента']));
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
    if ($parentName !== '') {
        $studentPayload['parentName'] = $parentName;
    }
    if ($childName !== '') {
        $studentPayload['childName'] = $childName;
    }
    if ($childBirthDate !== '') {
        $studentPayload['birthDate'] = $childBirthDate;
        $studentPayload['childBirthDate'] = $childBirthDate;
    }
    if ($language !== '') {
        $studentPayload['discipline'] = $language;
    }
    if ($level !== '') {
        $studentPayload['level'] = $level;
    }
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
    $extraFields = $payload['extra_fields'] ?? [];
    if (!is_array($extraFields)) {
        return null;
    }

    foreach ($keys as $key) {
        $value = $extraFields[$key] ?? null;
        if (is_scalar($value) && trim((string) $value) !== '') {
            return trim((string) $value);
        }
    }

    return null;
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

    $normalized = mb_strtolower($value, 'UTF-8');
    $normalized = str_replace('ё', 'е', $normalized);
    $normalized = preg_replace('/\s+/u', ' ', $normalized);

    $map = [
        'английский' => 'Английский',
        'english' => 'Английский',
        'китайский' => 'Китайский',
        'chinese' => 'Китайский',
        'корейский' => 'Корейский',
        'korean' => 'Корейский',
        'японский' => 'Японский',
        'japanese' => 'Японский',
        'турецкий' => 'Турецкий',
        'turkish' => 'Турецкий',
        'арабский' => 'Арабский',
        'arabic' => 'Арабский',
        'испанский' => 'Испанский',
        'spanish' => 'Испанский',
        'немецкий' => 'Немецкий',
        'german' => 'Немецкий',
        'французский' => 'Французский',
        'french' => 'Французский',
        'итальянский' => 'Итальянский',
        'italian' => 'Итальянский',
        'хинди' => 'Хинди',
        'hindi' => 'Хинди',
        'персидский' => 'Персидский',
        'persian' => 'Персидский',
        'иврит' => 'Иврит',
        'hebrew' => 'Иврит',
        'рки' => 'РКИ',
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

    return $value;
}
