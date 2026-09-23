<?php

declare(strict_types=1);

const EXPORT_SCRIPT_SOURCE = 'export_source_closed_statuses.php';
const SOURCE_AMO_ACCESS_TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6ImVmMjg5ODQ2YTBlZmE4MTEyYTgwYTUyZTcyOTU5M2FjM2NlMWFlMzE2ZmQ4ZTZlZDNkMjE0YjkxN2JmZmRlNmI4ZGRhZDYxNzQ1MmVhZDE5In0.eyJhdWQiOiIwNTQ0ZDY3NC00MmI4LTRiYTMtOTBlZC02OTM4MGFkMzNjNWQiLCJqdGkiOiJlZjI4OTg0NmEwZWZhODExMmE4MGE1MmU3Mjk1OTNhYzNjZTFhZTMxNmZkOGU2ZWQzZDIxNGI5MTdiZmZkZTZiOGRkYWQ2MTc0NTJlYWQxOSIsImlhdCI6MTc4NjE4NzMxNSwibmJmIjoxNzg2MTg3MzE1LCJleHAiOjE4NDYwMjI0MDAsInN1YiI6IjEyNjQyMzE0IiwiZ3JhbnRfdHlwZSI6IiIsImFjY291bnRfaWQiOjMyNDk1NjI2LCJiYXNlX2RvbWFpbiI6ImFtb2NybS5ydSIsInZlcnNpb24iOjIsInNjb3BlcyI6WyJwdXNoX25vdGlmaWNhdGlvbnMiLCJmaWxlcyIsImNybSIsImZpbGVzX2RlbGV0ZSIsIm5vdGlmaWNhdGlvbnMiXSwiaGFzaF91dWlkIjoiZGJkZGMwNjItOTAxYS00NjA0LTk3YTItYzc3OWRlMDZkYTZkIiwiYXBpX2RvbWFpbiI6ImFwaS1iLmFtb2NybS5ydSJ9.MdFT-9mnH3NzHqNeeiwLtaLLFBcx2bbz3S6wks7o43SHg1jCcI10DbwzCgVMfjtCOs666MIKPrQXcFFijzKfGI3AwuOK-3tOQafk-f9mLqy-3IqTjhFcnr-vaHSrqg1l6XmVi5W933YpTrWP7ocB-DVXHuzcG8GxuSdRP4VJfQEW0ZijGxX8x1OhPy7Nu_-9QmGqSObCoGHVXjNSJ-uxlO9anBJuNwCNj_meoa4mbRppUo6S0c-mi7ZPy-ab60UPJC3n3QTuuYXWMK3AuX7L-dIKH-i3TRhj9d9WFQiYBOTLTsZCEQzfMWTx8KzcnfjIlxhUIBI-6fK5CYGUi9Fwpg';
const SOURCE_AMO_BASE_URL = 'https://supportchinatutorru.amocrm.ru';
const EXPORT_JSON_FILE = __DIR__ . '/source_closed_statuses.json';
const EXPORT_LOG_FILE = __DIR__ . '/logs/export_source_closed_statuses.log';

try {
    $pipelines = export_fetch_all_pipelines();
    $matchedStatuses = [];

    foreach ($pipelines as $pipeline) {
        $pipelineId = (int) ($pipeline['id'] ?? 0);
        $pipelineName = trim((string) ($pipeline['name'] ?? ''));
        $statuses = $pipeline['_embedded']['statuses'] ?? [];

        foreach ($statuses as $status) {
            $statusId = (int) ($status['id'] ?? 0);
            $statusName = trim((string) ($status['name'] ?? ''));
            $classification = export_classify_status($status);

            if ($classification === null) {
                continue;
            }

            $matchedStatuses[] = [
                'pipeline_id' => $pipelineId,
                'pipeline_name' => $pipelineName,
                'status_id' => $statusId,
                'status_name' => $statusName,
                'classification' => $classification,
                'sort' => (int) ($status['sort'] ?? 0),
                'type' => (int) ($status['type'] ?? 0),
                'is_editable' => (bool) ($status['is_editable'] ?? false),
                'color' => (string) ($status['color'] ?? ''),
            ];
        }
    }

    usort(
        $matchedStatuses,
        static function (array $left, array $right): int {
            return [
                $left['pipeline_id'],
                $left['classification'],
                $left['sort'],
                $left['status_id'],
            ] <=> [
                $right['pipeline_id'],
                $right['classification'],
                $right['sort'],
                $right['status_id'],
            ];
        }
    );

    $payload = [
        'success' => true,
        'generated_at' => date(DATE_ATOM),
        'api_domain' => parse_url(SOURCE_AMO_BASE_URL, PHP_URL_HOST),
        'account_id' => export_extract_account_id_from_jwt(SOURCE_AMO_ACCESS_TOKEN),
        'pipelines_total' => count($pipelines),
        'matched_statuses_total' => count($matchedStatuses),
        'won_statuses_total' => count(array_filter($matchedStatuses, static fn (array $item): bool => $item['classification'] === 'won')),
        'lost_statuses_total' => count(array_filter($matchedStatuses, static fn (array $item): bool => $item['classification'] === 'lost')),
        'statuses' => array_values($matchedStatuses),
    ];

    export_ensure_directory(dirname(EXPORT_JSON_FILE));
    $json = json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

    if ($json === false) {
        throw new RuntimeException('Не удалось сериализовать JSON');
    }

    if (@file_put_contents(EXPORT_JSON_FILE, $json . PHP_EOL, LOCK_EX) === false) {
        throw new RuntimeException('Не удалось записать JSON в файл: ' . EXPORT_JSON_FILE);
    }

    export_log('INFO', 'Экспорт закрытых статусов завершен', [
        'pipelines_total' => count($pipelines),
        'matched_statuses_total' => count($matchedStatuses),
        'output_file' => EXPORT_JSON_FILE,
    ]);

    export_respond([
        'success' => true,
        'message' => 'JSON успешно сформирован',
        'output_file' => EXPORT_JSON_FILE,
        'pipelines_total' => count($pipelines),
        'matched_statuses_total' => count($matchedStatuses),
    ], 200);
} catch (Throwable $e) {
    export_log('ERROR', 'Ошибка экспорта закрытых статусов', [
        'error' => $e->getMessage(),
        'trace' => $e->getTraceAsString(),
    ]);

    export_respond([
        'success' => false,
        'error' => $e->getMessage(),
    ], 500);
}

function export_fetch_all_pipelines(): array
{
    $page = 1;
    $pipelines = [];

    do {
        $response = export_source_request('/api/v4/leads/pipelines?page=' . $page . '&limit=250');
        $pageItems = $response['_embedded']['pipelines'] ?? [];

        if (!is_array($pageItems) || $pageItems === []) {
            break;
        }

        foreach ($pageItems as $pipeline) {
            if (is_array($pipeline) && isset($pipeline['id'])) {
                $pipelines[] = $pipeline;
            }
        }

        $page++;
    } while (count($pageItems) === 250);

    return $pipelines;
}

function export_classify_status_name(string $statusName): ?string
{
    $normalized = export_normalize_name($statusName);

    if ($normalized === '') {
        return null;
    }

    if (str_contains($normalized, 'не реализ')) {
        return 'lost';
    }

    if (
        str_contains($normalized, 'закрыто реализ') ||
        str_contains($normalized, 'успешно реализ') ||
        str_contains($normalized, 'реализовано')
    ) {
        return 'won';
    }

    return null;
}

function export_classify_status(array $status): ?string
{
    $statusId = (int) ($status['id'] ?? 0);
    if ($statusId === 142) {
        return 'won';
    }

    if ($statusId === 143) {
        return 'lost';
    }

    return export_classify_status_name((string) ($status['name'] ?? ''));
}

function export_normalize_name(string $value): string
{
    $value = mb_strtolower(trim($value), 'UTF-8');
    $value = str_replace('ё', 'е', $value);
    $value = (string) preg_replace('/\s+/u', ' ', $value);
    return $value;
}

function export_source_request(string $path): array
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
        throw new RuntimeException('Ошибка запроса к AmoCRM: ' . $curlError);
    }

    if ($httpCode < 200 || $httpCode > 204) {
        throw new RuntimeException('AmoCRM вернул HTTP ' . $httpCode . ': ' . (string) $raw);
    }

    $decoded = json_decode((string) $raw, true);
    if (!is_array($decoded)) {
        throw new RuntimeException('AmoCRM вернул некорректный JSON');
    }

    return $decoded;
}

function export_extract_api_domain_from_jwt(string $jwt): string
{
    $payload = export_extract_jwt_payload($jwt);
    $apiDomain = is_array($payload) ? (string) ($payload['api_domain'] ?? '') : '';

    return $apiDomain !== '' ? $apiDomain : 'api-b.amocrm.ru';
}

function export_extract_account_id_from_jwt(string $jwt): int
{
    $payload = export_extract_jwt_payload($jwt);
    return (int) ($payload['account_id'] ?? 0);
}

function export_extract_jwt_payload(string $jwt): array
{
    $parts = explode('.', $jwt);
    if (count($parts) < 2) {
        return [];
    }

    $payloadPart = strtr($parts[1], '-_', '+/');
    $pad = strlen($payloadPart) % 4;
    if ($pad > 0) {
        $payloadPart .= str_repeat('=', 4 - $pad);
    }

    $payloadRaw = base64_decode($payloadPart, true);
    if ($payloadRaw === false) {
        return [];
    }

    $payload = json_decode($payloadRaw, true);
    return is_array($payload) ? $payload : [];
}

function export_respond(array $payload, int $statusCode): void
{
    if (!headers_sent()) {
        header('Content-Type: application/json; charset=utf-8');
        http_response_code($statusCode);
    }

    echo json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
}

function export_log(string $level, string $message, array $context = []): void
{
    export_ensure_directory(dirname(EXPORT_LOG_FILE));

    $line = '[' . date('Y-m-d H:i:s') . ']';
    $line .= ' [' . strtoupper($level) . ']';
    $line .= ' [' . EXPORT_SCRIPT_SOURCE . '] ' . $message;

    if ($context !== []) {
        $line .= "\n" . json_encode($context, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
    }

    $line .= "\n" . str_repeat('-', 80) . "\n";
    @file_put_contents(EXPORT_LOG_FILE, $line, FILE_APPEND | LOCK_EX);
}

function export_ensure_directory(string $directory): void
{
    if (!is_dir($directory)) {
        @mkdir($directory, 0755, true);
    }
}
