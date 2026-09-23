<?php

declare(strict_types=1);

function tests_project_root(): string
{
    return dirname(__DIR__);
}

function tests_assert_same($expected, $actual, string $message): void
{
    if ($expected !== $actual) {
        fwrite(STDERR, $message . PHP_EOL);
        fwrite(STDERR, 'Expected: ' . var_export($expected, true) . PHP_EOL);
        fwrite(STDERR, 'Actual:   ' . var_export($actual, true) . PHP_EOL);
        exit(1);
    }
}

function tests_assert_true(bool $condition, string $message): void
{
    if (!$condition) {
        fwrite(STDERR, $message . PHP_EOL);
        exit(1);
    }
}

function tests_assert_count(int $expectedCount, array $items, string $message): void
{
    tests_assert_same($expectedCount, count($items), $message);
}

function tests_assert_contains(string $needle, string $haystack, string $message): void
{
    if (strpos($haystack, $needle) === false) {
        fwrite(STDERR, $message . PHP_EOL);
        fwrite(STDERR, 'Needle: ' . $needle . PHP_EOL);
        fwrite(STDERR, 'Haystack: ' . $haystack . PHP_EOL);
        exit(1);
    }
}

function tests_load_functions(string $filePath, array $functionNames): void
{
    $source = file_get_contents($filePath);
    if ($source === false) {
        fwrite(STDERR, 'Cannot read file: ' . $filePath . PHP_EOL);
        exit(1);
    }

    foreach ($functionNames as $functionName) {
        if (function_exists($functionName)) {
            continue;
        }

        $pattern = '/function\s+' . preg_quote($functionName, '/') . '\s*\([^)]*\)\s*(?::\s*[^{\s]+)?\s*\{/m';
        if (!preg_match($pattern, $source, $matches, PREG_OFFSET_CAPTURE)) {
            fwrite(STDERR, 'Function not found: ' . $functionName . PHP_EOL);
            exit(1);
        }

        $start = $matches[0][1];
        $bracePosition = $start + strlen($matches[0][0]) - 1;
        $depth = 0;
        $length = strlen($source);
        $end = null;

        for ($i = $bracePosition; $i < $length; $i++) {
            if ($source[$i] === '{') {
                $depth++;
            } elseif ($source[$i] === '}') {
                $depth--;
                if ($depth === 0) {
                    $end = $i;
                    break;
                }
            }
        }

        if ($end === null) {
            fwrite(STDERR, 'Cannot parse function: ' . $functionName . PHP_EOL);
            exit(1);
        }

        $code = substr($source, $start, $end - $start + 1);
        eval($code);
    }
}
