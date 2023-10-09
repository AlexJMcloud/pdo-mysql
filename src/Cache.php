<?php
/**
 * PDOx - Useful Query Builder & PDO Class
 *
 * @class    Cache
 * @author   izni burak demirtaş (@izniburak) <info@burakdemirtas.org>
 * @web      <http://burakdemirtas.org>
 * @url      <https://github.com/izniburak/PDOx>
 * @license  The MIT License (MIT) - <http://opensource.org/licenses/MIT>
 */

namespace Buki;

use JsonException;
use RuntimeException;

class Cache
{
    protected string $cacheDir;
    protected int $cache;
    protected int $finish;

    /**
     * Cache constructor.
     *
     * @param string $dir
     * @param int  $time
     */
    public function __construct(string $dir = '', int $time = 0)
    {
        if (!file_exists($dir) && !mkdir($dir, 0755) && !is_dir($dir)) {
            throw new RuntimeException(sprintf('Directory "%s" was not created', $dir));
        }

        $this->cacheDir = $dir;
        $this->cache = $time;
        $this->finish = time() + $time;
        $this->clearCache();
    }

    /**
     * @param      $sql
     * @param bool $array
     * @return mixed
     * @throws JsonException
     */
    public function getCache($sql, bool $array = false):mixed
    {

        $cacheFile = $this->cacheDir . $this->fileName($sql) . '.cache';
        if (file_exists($cacheFile)) {
            $cache = json_decode(file_get_contents($cacheFile), $array, 512, JSON_THROW_ON_ERROR);

            if (($array ? $cache['finish'] : $cache->finish) < time()) {
                unlink($cacheFile);
                return false;
            }

            return $array ? $cache['data'] : $cache->data;
        }

        return false;
    }

    /**
     * @param $sql
     * @param $result
     *
     * @return bool
     * @throws JsonException
     */
    public function setCache($sql, $result):bool
    {

        $cacheFile = $this->cacheDir . $this->fileName($sql) . '.cache';
        $cacheFile = fopen($cacheFile, 'wb');

        if ($cacheFile) {
            fwrite($cacheFile, json_encode(['data' => $result, 'finish' => $this->finish], JSON_THROW_ON_ERROR));
        }

        return true;
    }

    /**
     * @param $name
     *
     * @return string
     */
    protected function fileName($name):string
    {
        return md5($name);
    }

    protected function clearCache():void
    {
        $files = glob($this->cacheDir.'/*');
        $timeCurrent=time();
        foreach ($files as $file) {
            $timeFile = filemtime($file);
            if(($timeCurrent - $timeFile) > 43200)
            {
                unlink($file);
            }
        }
    }
}
