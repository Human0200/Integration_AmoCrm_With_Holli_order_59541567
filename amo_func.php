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
$redirect_uri = 'https://srm.chinatutor.ru/hook.php';
$subdomain = 'directorchinatutorru';
$oauth_token = 'def50200cae7fac384409d01ff8393de246ae1c86f49675a37463d22235e9630bdca41f9d30083ca607dbc374e7662c0fd72e46d267b807c1a64a050cdf154409e73bb306e5443a3a0c5442ec7a183a930c023cc8003ad9d54bae75b8879ce8965f1d20c06af5355a82aec207a8a91e56861c25573732abecaaa2d47729228102610b9adce4efd491294f720405d312d55bd61f33e835f99c3d62343c6399e94d508b29a8f2e5c9dce81ca8a6466b020a0246627a9589340c7741e4ac23635f18a0a96be672bb616a321bcd4e2309c5ac10b049e79ca9efcdbd618763f58b3fb9fc1226815acdbf2a043d9bf93fb5ffddca3b256e8547440d022e29480c72ac195525bc581d2ed006595fe732c5feb69e766ca13524043e8d255c190c704382ff6b00fa62df420417bb530ca58a32f917d00f3bf9fe903cff3f425b472e3fe8d5d6e6b9b69ec273837dce83b172bd263a23b37abfa529e9b9c32bc7b62e09b7eeadbfbfa6b853ca71e088feda44f0dae8a03c5296a56acfc179f3d803d0a646a9fe71acd69f6c7d3fe9e76d77f028088d5a85852752f92e3ec2a1b678ca27f0b447d18b7f10da5ba90a608dde599b0d572e5f172d62cf93352f4a7260b1a9bca14d81d019e412692885f72d68d4b1286538aaa58b8c8bd7063bf73e21b0d0ac2c3d6eca1f1df0a10584b338a8006ce743e27740f';

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


