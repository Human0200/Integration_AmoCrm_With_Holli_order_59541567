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

// Подключаем логгер, если он еще не подключен
if (!function_exists('log_message')) {
    if (file_exists(__DIR__ . '/logger.php')) {
        require_once __DIR__ . '/logger.php';
    }
}

$client_id = '423c1efc-1b6e-470d-ae84-34f69b4adf25';
$client_secret = 's2pQFJkE6A347PvGyz5eJeyHAvKP2stfXvoSNSpKKnZyW4NfcBUFXp7rGfm7xXnq';
$redirect_uri = 'https://directorchinatutorru.amocrm.ru/';
$subdomain = 'directorchinatutorru';
$oauth_token = 'def502007797bee6dcdcb7a32183af5f8b9572c87710f1d15f91131af4f0fa5dbc03eaa18659956bcaaebdd381dd41843a2b4588d73cfa3693e9348baea27e0c64e6850282b7caeec6eaa2246594dd41fd1e8024986f9deef06060e7cc44e883434d5052d3d5910cc860deae9ae7a12a1a6ae3ef588b3be397b418837ddeaa7db43709a49f32417ede995236f1f14a0ff3303d702706c8d72dd32ed78d39d43698bf1e8df5b00eb4bd0410e80c5ac74b506e32c461e24fa71b3ee79d76acaa0b52878c37518f0840a2db15696c00e17f6c631142a54fe962c104d4c0dfb5b434f63c589f57f283cd5729982aee6a197f7d530825295794d669d819914899760d771b67c637ffd0148658770c1705007186702df4dc48068690896e8d30b859d6641b355105c6177cb5d89c31f63a4690484506e28c3de0b159ebbeb5b116a088445f4c3fa0861fe3d33762475469386cdd365d18dc438e71976d127195d8e3c240c5ac91be07563d47244868b9ab372997eb7e0611ee5e65de6d149d5532df432b8a9db3abba175ae1ab2b45992be9a692a0922aaaafeea7eea8456e11bc982e0acc1df4236aa1d160fd385b242d47e67c1c9e2ae547417816c2dd6a82a9e932724fcb861a76477f7feb53e1ac0c97901ae661c37fea417a51e90cde91c8a4c5dce95aef81fba389425dd18bee866a847b1701e0';

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

function get ($subdomain, $url, $data) {
    $link = 'https://' . $subdomain . '.amocrm.ru'.$url;
    $access_token = $data['access_token'];
    $headers = [
        'Authorization: Bearer ' . $access_token
    ];
    // echo print_r($headers).'<br><br>';
    $curl = curl_init(); //Сохраняем дескриптор сеанса cURL
    curl_setopt($curl,CURLOPT_RETURNTRANSFER, true);
    curl_setopt($curl,CURLOPT_USERAGENT,'amoCRM-oAuth-client/1.0');
    curl_setopt($curl,CURLOPT_URL, $link);
    curl_setopt($curl,CURLOPT_HTTPHEADER, $headers);
    curl_setopt($curl,CURLOPT_HEADER, false);
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
    return $result;
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
    $curl = curl_init(); //Сохраняем дескриптор сеанса cURL
    curl_setopt($curl,CURLOPT_RETURNTRANSFER, true);
    curl_setopt($curl,CURLOPT_USERAGENT,'amoCRM-oAuth-client/1.0');
    curl_setopt($curl,CURLOPT_URL, $link);
    curl_setopt($curl, CURLOPT_CUSTOMREQUEST, $method);
    curl_setopt($curl, CURLOPT_POSTFIELDS, json_encode($query_data));
    curl_setopt($curl,CURLOPT_HTTPHEADER, $headers);
    curl_setopt($curl,CURLOPT_HEADER, false);
    curl_setopt($curl,CURLOPT_SSL_VERIFYPEER, 1);
    curl_setopt($curl,CURLOPT_SSL_VERIFYHOST, 2);
    $out = curl_exec($curl); //Инициируем запрос к API и сохраняем ответ в переменную
    $code = curl_getinfo($curl, CURLINFO_HTTP_CODE);
    // echo $code.'<br>';
    // echo $out.'<br>';
    curl_close($curl);
    $code = (int)$code;
    $errors = array(
        301 => 'Moved permanently',
        400 => 'Bad request',
        401 => 'Unauthorized',
        403 => 'Forbidden',
        404 => 'Not found',
        500 => 'Internal server error',
        502 => 'Bad gateway',
        503 => 'Service unavailable',
    );
    try
    {
        if ($code != 200 && $code != 204) {
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
    return $result;
}


