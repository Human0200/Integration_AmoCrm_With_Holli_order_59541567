<?php

/**
$res = post_or_patch (
    $subdomain,
    array(
        'id' => $lead_id,
        'status_id' => 46017067,
        'responsible_user_id' => $amo_responsible_user_id
    ),
    '/api/v4/leads/'.$lead_id,
    $data,
    'PATCH'
);

$res = post_or_patch (
    $subdomain,
    array(
        array(
            'name' => $site,
            'status_id' => 46017064,
            'responsible_user_id' => $callback_code-100000000000000,
        )
    ),
    '/api/v4/leads',
    $data,
    'POST'
);

$api_url = '/api/v4/users';
$amo_users = get ($subdomain, $api_url, $data);
**/
date_default_timezone_set("Europe/Moscow");

require_once __DIR__ . '/config.php';

// Подключаем логгер, если он еще не подключен
if (!function_exists('log_message')) {
    if (file_exists(__DIR__ . '/logger.php')) {
        require_once __DIR__ . '/logger.php';
    }
}

$client_id = (string) get_config('amocrm.client_id');
$client_secret = (string) get_config('amocrm.client_secret');
$redirect_uri = (string) get_config('amocrm.redirect_uri');
$subdomain = (string) get_config('amocrm.subdomain');
$oauth_token = (string) ($_GET['code'] ?? '');

// auth
$data = json_decode(file_get_contents(__DIR__.'/tokens.json'), 1);
if (time() - (int) $data['time'] > 82800) {
    $link = 'https://' . $subdomain . '.amocrm.ru/oauth2/access_token';
    $refresh_data = [
        'client_id' => $client_id,
        'client_secret' => $client_secret,
        'grant_type' => 'refresh_token',
        'refresh_token' => $data['refresh_token'],
        'redirect_uri' => $redirect_uri,
    ];
    $curl = curl_init(); //Сохраняем дескриптор сеанса cURL
    curl_setopt($curl,CURLOPT_RETURNTRANSFER, true);
    curl_setopt($curl,CURLOPT_USERAGENT,'amoCRM-oAuth-client/1.0');
    curl_setopt($curl,CURLOPT_URL, $link);
    curl_setopt($curl,CURLOPT_HTTPHEADER,['Content-Type:application/json']);
    curl_setopt($curl,CURLOPT_HEADER, false);
    curl_setopt($curl,CURLOPT_CUSTOMREQUEST, 'POST');
    curl_setopt($curl,CURLOPT_POSTFIELDS, json_encode($refresh_data));
    curl_setopt($curl,CURLOPT_SSL_VERIFYPEER, 1);
    curl_setopt($curl,CURLOPT_SSL_VERIFYHOST, 2);
    $out = curl_exec($curl); //Инициируем запрос к API и сохраняем ответ в переменную
    $code = curl_getinfo($curl, CURLINFO_HTTP_CODE);
    curl_close($curl);
    $code = (int)$code;
    $errors = [
        400 => 'Bad request',
        401 => 'Unauthorized',
        403 => 'Forbidden',
        404 => 'Not found',
        500 => 'Internal server error',
        502 => 'Bad gateway',
        503 => 'Service unavailable',
    ];
    try
    {
        if ($code < 200 || $code > 204) {
            if (function_exists('log_error')) {
                log_error("Ошибка обновления токена AmoCRM", ['code' => $code, 'error' => isset($errors[$code]) ? $errors[$code] : 'Undefined error', 'response' => $out], 'amo_func.php');
            }
            throw new Exception(isset($errors[$code]) ? $errors[$code] : 'Undefined error', $code);
        }
        if (function_exists('log_info')) {
            log_info("Токен AmoCRM успешно обновлен", ['code' => $code], 'amo_func.php');
        }
    }
    catch(\Exception $e)
    {
        if (function_exists('log_error')) {
            log_error("Критическая ошибка обновления токена AmoCRM", ['error' => $e->getMessage(), 'code' => $e->getCode()], 'amo_func.php');
        }
        die('Ошибка: ' . $e->getMessage() . PHP_EOL . 'Код ошибки: ' . $e->getCode());
    }
    $response = json_decode($out, true);
    $response['time'] = time();
    file_put_contents(__DIR__.'/tokens.json', json_encode($response));
    $data['access_token'] = $response['access_token'];
}
// auth

function amo_api_rate_limit_delay_seconds(): float
{
    return 0.2;
}

function amo_api_max_retry_attempts(): int
{
    return 4;
}

function amo_api_wait_for_slot(): void
{
    $lockDir = __DIR__ . '/locks';
    if (!is_dir($lockDir)) {
        @mkdir($lockDir, 0755, true);
    }

    $lockFile = $lockDir . '/amo_api_rate_limit.lock';
    $fp = @fopen($lockFile, 'c+');
    if (!$fp) {
        usleep((int) round(amo_api_rate_limit_delay_seconds() * 1000000));
        return;
    }

    if (!flock($fp, LOCK_EX)) {
        fclose($fp);
        usleep((int) round(amo_api_rate_limit_delay_seconds() * 1000000));
        return;
    }

    $contents = stream_get_contents($fp);
    $lastRequestAt = is_string($contents) ? (float) trim($contents) : 0.0;
    $now = microtime(true);
    $minInterval = amo_api_rate_limit_delay_seconds();
    $elapsed = $now - $lastRequestAt;

    if ($lastRequestAt > 0 && $elapsed < $minInterval) {
        usleep((int) round(($minInterval - $elapsed) * 1000000));
    }

    $currentTime = microtime(true);
    ftruncate($fp, 0);
    rewind($fp);
    fwrite($fp, sprintf('%.6f', $currentTime));
    fflush($fp);
    flock($fp, LOCK_UN);
    fclose($fp);
}

function amo_api_retry_delay_seconds(int $attempt): float
{
    $delays = [
        1 => 0.5,
        2 => 1.0,
        3 => 1.5,
        4 => 2.0,
    ];

    return $delays[$attempt] ?? 2.5;
}

function get ($subdomain, $url, $data) {
    $link = 'https://' . $subdomain . '.amocrm.ru'.$url;
    $access_token = $data['access_token'];
    $headers = [
        'Authorization: Bearer ' . $access_token
    ];
    $errors = [
        400 => 'Bad request',
        401 => 'Unauthorized',
        403 => 'Forbidden',
        404 => 'Not found',
        429 => 'Too Many Requests',
        500 => 'Internal server error',
        502 => 'Bad gateway',
        503 => 'Service unavailable',
    ];

    $out = '';
    $code = 0;

    for ($attempt = 1; $attempt <= amo_api_max_retry_attempts(); $attempt++) {
        amo_api_wait_for_slot();

        $curl = curl_init(); //Сохраняем дескриптор сеанса cURL
        curl_setopt($curl,CURLOPT_RETURNTRANSFER, true);
        curl_setopt($curl,CURLOPT_USERAGENT,'amoCRM-oAuth-client/1.0');
        curl_setopt($curl,CURLOPT_URL, $link);
        curl_setopt($curl,CURLOPT_HTTPHEADER, $headers);
        curl_setopt($curl,CURLOPT_HEADER, false);
        curl_setopt($curl,CURLOPT_SSL_VERIFYPEER, 1);
        curl_setopt($curl,CURLOPT_SSL_VERIFYHOST, 2);
        $out = curl_exec($curl); //Инициируем запрос к API и сохраняем ответ в переменную
        $code = (int) curl_getinfo($curl, CURLINFO_HTTP_CODE);
        curl_close($curl);

        if ($code !== 429) {
            break;
        }

        if (function_exists('log_warning')) {
            log_warning("AmoCRM вернул 429 на GET, повторяем запрос", [
                'attempt' => $attempt,
                'url' => $url,
                'retry_in_seconds' => amo_api_retry_delay_seconds($attempt),
            ], 'amo_func.php');
        }

        if ($attempt < amo_api_max_retry_attempts()) {
            usleep((int) round(amo_api_retry_delay_seconds($attempt) * 1000000));
        }
    }

    try
    {
        if ($code < 200 || $code > 204) {
            if (function_exists('log_error')) {
                log_error("Ошибка GET запроса к AmoCRM API", ['code' => $code, 'url' => $url, 'error' => isset($errors[$code]) ? $errors[$code] : 'Undefined error', 'response' => substr($out, 0, 500)], 'amo_func.php');
            }
            throw new Exception(isset($errors[$code]) ? $errors[$code] : 'Undefined error', $code);
        }
        if (function_exists('log_debug')) {
            log_debug("GET запрос к AmoCRM API выполнен успешно", ['code' => $code, 'url' => $url], 'amo_func.php');
        }
    }
    catch(\Exception $e)
    {
        if (function_exists('log_error')) {
            log_error("Исключение при GET запросе к AmoCRM API", ['error' => $e->getMessage(), 'code' => $e->getCode(), 'url' => $url], 'amo_func.php');
        }
        die('Ошибка: ' . $e->getMessage() . PHP_EOL . 'Код ошибки: ' . $e->getCode());
    }
    // if ($url == '/api/v4/users') {
    //     echo $out.'<br>';
    // }
    $result = json_decode($out, true);
    return is_array($result) ? $result : (($code >= 200 && $code <= 204) ? [] : null);
}

function post_or_patch ($subdomain, $query_data, $url, $data, $method) {
    // echo 'POST:<br>';
    // echo $url.'<br>';
    $link = 'https://' . $subdomain . '.amocrm.ru'.$url;
    $access_token = $data['access_token'];
    $headers = [
        'Authorization: Bearer ' . $access_token,
        'Content-Type: application/json',
    ];
    $payload = json_encode($query_data);
    $errors = array(
        301 => 'Moved permanently',
        400 => 'Bad request',
        401 => 'Unauthorized',
        403 => 'Forbidden',
        404 => 'Not found',
        429 => 'Too Many Requests',
        500 => 'Internal server error',
        502 => 'Bad gateway',
        503 => 'Service unavailable',
    );

    $out = '';
    $code = 0;

    for ($attempt = 1; $attempt <= amo_api_max_retry_attempts(); $attempt++) {
        amo_api_wait_for_slot();

        $curl = curl_init(); //Сохраняем дескриптор сеанса cURL
        curl_setopt($curl,CURLOPT_RETURNTRANSFER, true);
        curl_setopt($curl,CURLOPT_USERAGENT,'amoCRM-oAuth-client/1.0');
        curl_setopt($curl,CURLOPT_URL, $link);
        curl_setopt($curl, CURLOPT_CUSTOMREQUEST, $method);
        curl_setopt($curl, CURLOPT_POSTFIELDS, $payload);
        curl_setopt($curl,CURLOPT_HTTPHEADER, $headers);
        curl_setopt($curl,CURLOPT_HEADER, false);
        curl_setopt($curl,CURLOPT_SSL_VERIFYPEER, 1);
        curl_setopt($curl,CURLOPT_SSL_VERIFYHOST, 2);
        $out = curl_exec($curl); //Инициируем запрос к API и сохраняем ответ в переменную
        $code = (int) curl_getinfo($curl, CURLINFO_HTTP_CODE);
        curl_close($curl);

        if ($code !== 429) {
            break;
        }

        if (function_exists('log_warning')) {
            log_warning("AmoCRM вернул 429 на {$method}, повторяем запрос", [
                'attempt' => $attempt,
                'url' => $url,
                'retry_in_seconds' => amo_api_retry_delay_seconds($attempt),
            ], 'amo_func.php');
        }

        if ($attempt < amo_api_max_retry_attempts()) {
            usleep((int) round(amo_api_retry_delay_seconds($attempt) * 1000000));
        }
    }

    try
    {
        if ($code < 200 || $code > 204) {
            if (function_exists('log_error')) {
                log_error("Ошибка {$method} запроса к AmoCRM API", ['code' => $code, 'method' => $method, 'url' => $url, 'error' => isset($errors[$code]) ? $errors[$code] : 'Undescribed error', 'response' => substr($out, 0, 500)], 'amo_func.php');
            }
            throw new Exception(isset($errors[$code]) ? $errors[$code] : 'Undescribed error', $code);
        }
        if (function_exists('log_debug')) {
            log_debug("{$method} запрос к AmoCRM API выполнен успешно", ['code' => $code, 'method' => $method, 'url' => $url], 'amo_func.php');
        }
    } catch (Exception $E) {
        if (function_exists('log_error')) {
            log_error("Исключение при {$method} запросе к AmoCRM API", ['error' => $E->getMessage(), 'code' => $E->getCode(), 'method' => $method, 'url' => $url], 'amo_func.php');
        }
        die('Ошибка: ' . $E->getMessage() . PHP_EOL . 'Код ошибки: ' . $E->getCode());
    }
    $result = json_decode($out, true);
    return is_array($result) ? $result : (($code >= 200 && $code <= 204) ? [] : null);
}
