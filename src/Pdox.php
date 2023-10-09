<?php

namespace Buki;

use Closure;
use JsonException;
use PDO;
use PDOException;

/**
 * PDOx - Useful Query Builder & PDO Class
 *
 * @package  Pdox
 * @author   izni burak demirtaş (@izniburak) <info@burakdemirtas.org>
 * @url      <https://github.com/izniburak/PDOx>
 * @license  The MIT License (MIT) - <http://opensource.org/licenses/MIT>
 */
class Pdox implements PdoxInterface

    /**
     * @var PDO
     */
    public mixed $pdo;

    /**
     * @var mixed Query variables
     */
    protected mixed $select = '*';
    protected mixed $from = null;
    protected mixed $where = null;
    protected mixed $limit = null;
    protected mixed $offset = null;
    protected mixed $join = null;
    protected mixed $orderBy = null;
    protected mixed $groupBy = null;
    protected mixed $having = null;
    protected mixed $grouped = false;
    protected int $numRows;
    protected mixed $insertId = null;
    protected mixed $query = null;
    protected mixed $error = null;
    protected mixed $result = [];
    protected mixed $prefix = null;

    /**
     * @var array SQL operators
     */
    protected array $operators = ['=', '!=', '<', '>', '<=', '>=', '<>', 'RLIKE'];

    /**
     * @var Cache|null
     */
    protected ?Cache $cache = null;

    /**
     * @var string|null Cache Directory
     */
    protected mixed $cacheDir = null;

    /**
     * @var int Total query count
     */
    protected int $queryCount = 0;

    /**
     * @var bool
     */
    protected mixed $debug = true;

    /**
     * @var int Total transaction count
     */
    protected int $transactionCount = 0;

    /**
     * DB constructor.
     *
     * @param array $config
     */
    public function __construct(array $config)
    {
        $config['driver'] = $config['driver'] ?? 'mysql';
        $config['host'] = $config['host'] ?? 'localhost';
        $config['charset'] = $config['charset'] ?? 'utf8mb4';
        $config['collation'] = $config['collation'] ?? 'utf8mb4_general_ci';
        $config['port'] = $config['port'] ?? (str_contains($config['host'], ':') ? explode(':', $config['host'])[1] : '');
        $this->prefix = $config['prefix'] ?? '';
        $this->cacheDir = $config['cachedir'] ?? (__DIR__ . '/cache/');
        $this->debug = $config['debug'] ?? true;

        $dsn = '';
        if (in_array($config['driver'], ['', 'mysql', 'pgsql'], true)) {
            $dsn = $config['driver'] . ':host=' . str_replace(':' . $config['port'], '', $config['host']) . ';'
                . ($config['port'] !== '' ? 'port=' . $config['port'] . ';' : '')
                . 'dbname=' . $config['database'];
        } elseif ($config['driver'] === 'sqlite') {
            $dsn = 'sqlite:' . $config['database'];
        } elseif ($config['driver'] === 'oracle') {
            $dsn = 'oci:dbname=' . $config['host'] . '/' . $config['database'];
        }

        try {
            $this->pdo = new PDO($dsn, $config['username'], $config['password'],array(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_OBJ, PDO::ATTR_PERSISTENT));
            $this->pdo->exec("SET NAMES '" . $config['charset'] . "' COLLATE '" . $config['collation'] . "'");
            $this->pdo->exec("SET CHARACTER SET '" . $config['charset'] . "'");
        } catch (PDOException $e) {
            die('Cannot the connect to Database with PDO. ' . $e->getMessage());
        }
    }

    /**
     * @param mixed $table
     *
     * @return static
     */
    public function table(mixed $table): static
    {
        if (is_array($table)) {
            $from = '';
            foreach ($table as $key) {
                $from .= '`' .$this->prefix . $key . '`, ';
            }
            $this->from = rtrim($from, ', ');
        } else if (strpos($table, ',') > 0) {
            $tables = explode(',', $table);
            if(is_array($tables))
            {
                $tables = array_map(function ($value) {return '`' .$this->prefix . ltrim($value).'`';}, $tables);
                /*foreach ($tables as $value) {
                    $value = $this->prefix . ltrim($value);
                }*/
                $this->from = implode(', ', $tables);
            }
        } else {
            $this->from = '`' . $this->prefix . $table . '`';
        }

        return $this;
    }
    /**
     * @param array|string $fields
     *
     * @return static
     */
    public function select(mixed $fields): static
    {
        $select = is_array($fields) ? implode(', ', $fields) : $fields;
        $this->optimizeSelect($select);

        return $this;
    }

    /**
     * @param string      $field
     * @param string|null $name
     *
     * @return static
     */
    public function max(string $field, mixed $name = null):static
    {
        $column = 'MAX(' . $field . ')' . (!is_null($name) ? ' AS ' . $name : '');
        $this->optimizeSelect($column);

        return $this;
    }

    /**
     * @param string      $field
     * @param string|null $name
     *
     * @return static
     */
    public function min(string $field, mixed $name = null):static
    {
        $column = 'MIN(' . $field . ')' . (!is_null($name) ? ' AS ' . $name : '');
        $this->optimizeSelect($column);

        return $this;
    }

    /**
     * @param string      $field
     * @param string|null $name
     *
     * @return static
     */
    public function sum(string $field, mixed $name = null):static
    {
        $column = 'SUM(' . $field . ')' . (!is_null($name) ? ' AS ' . $name : '');
        $this->optimizeSelect($column);

        return $this;
    }

    /**
     * @param string      $field
     * @param string|null $name
     *
     * @return static
     */
    public function count(string $field, mixed $name = null):static
    {
        $column = 'COUNT(' . $field . ')' . (!is_null($name) ? ' AS ' . $name : '');
        $this->optimizeSelect($column);

        return $this;
    }

    /**
     * @param string      $field
     * @param string|null $name
     *
     * @return static
     */
    public function avg(string $field, mixed $name = null):static
    {
        $column = 'AVG(' . $field . ')' . (!is_null($name) ? ' AS ' . $name : '');
        $this->optimizeSelect($column);

        return $this;
    }

    /**
     * @param string|array $field
     * @param string|array $params
     * @param string $andOr
     * @return $this
     */
    public function match(string|array $field, string|array $params, string $andOr = 'AND'):static
    {
        if(empty($field) && empty($params))
        {
            return $this;
        }
        $_field = (is_array($field)) ? implode(',', $field) : '`' . $field . '`';
        if(is_array($params))
        {
            foreach ($params as $param)
            {
                preg_match('/\s/', $param,$math);
                if(empty($math))
                {
                    $param = htmlspecialchars($param).'*';
                }
                else
                {
                    $tmp = preg_split('/\s/', $param);
                    $i = 0;
                    foreach ($tmp as $_tmp)
                    {
                        if(strlen($_tmp) > 3)
                        {
                            $_tmp = htmlspecialchars($_tmp).'*';
                        }
                        else
                            if($i > 0)
                            {
                                $tmp[($i - 1)] .= ' ' . $_tmp.'*';
                                unset($_tmp);
                            }
                            else
                            {
                                $_tmp = $tmp[($i - 1)] . ' ' . $_tmp.'*';
                                unset($tmp[($i - 1)]);
                            }
                        $i++;
                    }
                    $param = implode('+', $tmp);
                }
            }
            $_param = implode('+',$params);
        }
        else {
            $_param = '+' . htmlspecialchars($params).'*';
        }
        $where = 'MATCH (' . $_field . ') AGAINST(\'' . $_param . '\' IN BOOLEAN MODE)';

        $this->where = is_null($this->where)
            ? $where
            : $this->where . ' ' . $andOr . ' ' . $where;

        return $this;
    }

    /**
     * @param string      $table
     * @param string|null $field1
     * @param string|null $operator
     * @param string|null $field2
     * @param string      $type
     *
     * @return static
     */
    public function join(string $table, mixed $field1 = null, mixed $operator = null, mixed $field2 = null, string $type = ''):static
    {
        $on = $field1;
        if(str_contains($table, ' as ') !== false)
        {
            $__tmp = explode(' ', $table);
            $table = $__tmp[0];
            $table = '`' . $this->prefix . $table . '` as ' . $__tmp[3];
        }
        else
        {
            $table = '`' . $this->prefix . $table . '`';
        }
        if (!is_null($operator) && !is_null($on)) {
            $on = !in_array($operator, $this->operators, true)
                ? $field1 . ' = ' . $operator . (!is_null($field2) ? ' ' . $field2 : '')
                : $field1 . ' ' . $operator . ' ' . $field2;
        }
        if(!is_null($on))
        {
            $this->join = (is_null($this->join))
                ? ' ' . $type . 'JOIN' . ' ' . $table . ' ON ' . $on
                : $this->join . ' ' . $type . 'JOIN' . ' ' . $table . ' ON ' . $on;
        }
        else
        {
            $this->join = (is_null($this->join)) ? ' ' . $type . 'JOIN' . ' ' . $table: $this->join . ' ' . $type . 'JOIN' . ' ' . $table;
        }
        return $this;
    }

    /**
     * @param string $table
     * @param string $field1
     * @param string|null $operator
     * @param string $field2
     *
     * @return static
     */
    public function innerJoin(string $table, string $field1, string|null $operator = null, string $field2 = ''): static
    {
        return $this->join($table, $field1, $operator, $field2, 'INNER ');
    }

    /**
     * @param string $table
     * @param string $field1
     * @param string|null $operator
     * @param string $field2
     *
     * @return static
     */
    public function leftJoin(string $table, string $field1, string|null $operator = null, string $field2 = ''):static
    {
        return $this->join($table, $field1, $operator, $field2, 'LEFT ');
    }

    /**
     * @param string $table
     * @param string $field1
     * @param string $operator
     * @param string $field2
     *
     * @return static
     */
    public function rightJoin(string $table, string $field1, string $operator = '', string $field2 = ''): static
    {
        return $this->join($table, $field1, $operator, $field2, 'RIGHT ');
    }

    /**
     * @param string $table
     * @param string $field1
     * @param string $operator
     * @param string $field2
     *
     * @return static
     */
    public function fullOuterJoin(string $table, string $field1, string $operator = '', string $field2 = ''): static
    {
        return $this->join($table, $field1, $operator, $field2, 'FULL OUTER ');
    }

    /**
     * @param string $table
     * @param string $field1
     * @param string $operator
     * @param string $field2
     *
     * @return static
     */
    public function leftOuterJoin(string $table, string $field1, string $operator = '', string $field2 = ''): static
    {
        return $this->join($table, $field1, $operator, $field2, 'LEFT OUTER ');
    }

    /**
     * @param string $table
     * @param string $field1
     * @param string $operator
     * @param string $field2
     *
     * @return static
     */
    public function rightOuterJoin(string $table, string $field1, string $operator = '', string $field2 = ''): static
    {
        return $this->join($table, $field1, $operator, $field2, 'RIGHT OUTER ');
    }

    /**
     * @param array|string $where
     * @param string       $operator
     * @param string       $val
     * @param string       $type
     * @param string       $andOr
     *
     * @return static
     */
    public function where(mixed $where, mixed $operator = null, mixed $val = null, string $type = '', string $andOr = 'AND'): static
    {
        if (is_array($where) && !empty($where)) {
            $_where = [];
            foreach ($where as $column => $data) {
                $_where[] = $type . $column . '=' . $this->escape($data);
            }
            $where = implode(' ' . $andOr . ' ', $_where);
        } else {
            if (empty($where)) {
                return $this;
            }

            if (is_array($operator)) {
                $params = explode('?', $where);
                $_where = '';
                foreach ($params as $key => $value) {
                    if (!empty($value)) {
                        $_where .= $type . $value . (isset($operator[$key]) ? $this->escape($operator[$key]) : '');
                    }
                }
                $where = $_where;
            } elseif(empty($operator) && empty($val)){
                $where = $type . $where;
            } elseif (!in_array($operator, $this->operators, true) || !$operator) {
                $where = $type . $where . ' = ' . $this->escape($operator);
            }
            else
            {
                $where = $type . $where . ' ' . $operator . ' ' . $this->escape($val);
            }
        }

        if ($this->grouped) {
            $where = '(' . $where;
            $this->grouped = false;
        }

        $this->where = is_null($this->where)
            ? $where
            : $this->where . ' ' . $andOr . ' ' . $where;

        return $this;
    }

    /**
     * @param array|string $where
     * @param string|null $operator
     * @param string|null $val
     *
     * @return static
     */
    public function orWhere(array|string $where, string $operator = null, string $val = null): static
    {
        return $this->where($where, $operator, $val, '', 'OR');
    }

    /**
     * @param array|string $where
     * @param string|null $operator
     * @param string|null $val
     *
     * @return static
     */
    public function notWhere(array|string $where, string $operator = null, string $val = null): static
    {
        return $this->where($where, $operator, $val, 'NOT ');
    }

    /**
     * @param array|string $where
     * @param string|null $operator
     * @param string|null $val
     *
     * @return static
     */
    public function orNotWhere(array|string $where, string $operator = null, string $val = null): static
    {
        return $this->where($where, $operator, $val, 'NOT ', 'OR');
    }

    /**
     * @param string $where
     * @param bool $not
     *
     * @return static
     */
    public function whereNull(string $where, bool $not = false): static
    {
        $where .= ' IS ' . ($not ? 'NOT' : '') . ' NULL';
        $this->where = is_null($this->where) ? $where : $this->where . ' ' . 'AND ' . $where;

        return $this;
    }

    /**
     * @param string $where
     *
     * @return static
     */
    public function whereNotNull(string $where): static
    {
        return $this->whereNull($where, true);
    }

    /**
     * @param Closure $obj
     *
     * @return static
     */
    public function grouped(Closure $obj): static
    {
        $this->grouped = true;
        $obj($this);
        $this->where .= ')';

        return $this;
    }

    /**
     * @param string $field
     * @param array|string $keys
     * @param string $type
     * @param string $andOr
     *
     * @return static
     */
    public function in(string $field, array|string $keys, string $type = '', string $andOr = 'AND'): static
    {
        $_keys = [];
        if(is_array($keys))
        {
            foreach ($keys as $v) {
                $_keys[] = is_numeric($v) ? $v : $this->escape($v);
            }
            $where = $field . ' ' . $type . 'IN (' . implode(', ', $_keys) . ')';
        }
        else
        {
            $where = $field . ' ' . $type . 'IN (' . $this->escape($keys) . ')';
        }

        if ($this->grouped) {
            $where = '(' . $where;
            $this->grouped = false;
        }
        $this->where = is_null($this->where)
            ? $where
            : $this->where . ' ' . $andOr . ' ' . $where;
        return $this;
    }

    /**
     * @param string $field
     * @param array  $keys
     *
     * @return static
     */
    public function notIn(string $field, array $keys): static
    {
        return $this->in($field, $keys, 'NOT ');
    }

    /**
     * @param string $field
     * @param array  $keys
     *
     * @return static
     */
    public function orIn(string $field, array $keys): static
    {
        return $this->in($field, $keys, '', 'OR');
    }

    /**
     * @param string $field
     * @param array  $keys
     *
     * @return static
     */
    public function orNotIn(string $field, array $keys): static
    {
        return $this->in($field, $keys, 'NOT ', 'OR');
    }

    /**
     * @param string $field
     * @param int|float|string $key
     * @param string $type
     * @param string $andOr
     *
     * @return static
     */
    public function findInSet(string $field, int|float|string $key, string $type = '', string $andOr = 'AND'): static
    {
        $key = is_numeric($key) ? (int)$key : $this->escape($key);
        $where =  $type . 'FIND_IN_SET (' . $key . ', '.$field.')';

        if ($this->grouped) {
            $where = '(' . $where;
            $this->grouped = false;
        }

        $this->where = is_null($this->where)
            ? $where
            : $this->where . ' ' . $andOr . ' ' . $where;

        return $this;
    }

    /**
     * @param string $field
     * @param string $key
     *
     * @return static
     */
    public function notFindInSet(string $field, string $key): static
    {
        return $this->findInSet($field, $key, 'NOT ');
    }

    /**
     * @param string $field
     * @param string $key
     *
     * @return static
     */
    public function orFindInSet(string $field, string $key): static
    {
        return $this->findInSet($field, $key, '', 'OR');
    }

    /**
     * @param string $field
     * @param string $key
     *
     * @return static
     */
    public function orNotFindInSet(string $field, string $key): static
    {
        return $this->findInSet($field, $key, 'NOT ', 'OR');
    }

    /**
     * @param string $field
     * @param mixed $value1
     * @param int|string $value2
     * @param string $type
     * @param string $andOr
     *
     * @return static
     */
    public function between(string $field, mixed $value1, int|string $value2 = '', string $type = '', string $andOr = 'AND'): static
    {
        if(is_array($value1) && count($value1) === 2)
        {
            $where = '(' . $field . ' ' . $type . 'BETWEEN ' . $value1[0] . ' AND ' . $value1[1] . ')';
        }
        elseif(!empty($value2))
        {
            $where = '(' . $field . ' ' . $type . 'BETWEEN ' . ($this->escape($value1) . ' AND ' . $this->escape($value2)) . ')';
        }
        if(!empty($where))
        {
            if ($this->grouped) {
                $where = '(' . $where;
                $this->grouped = false;
            }

            $this->where = is_null($this->where)
                ? $where
                : $this->where . ' ' . $andOr . ' ' . $where;
        }
        return $this;
    }

    /**
     * @param string $field
     * @param int|string $value1
     * @param int|string $value2
     *
     * @return static
     */
    public function notBetween(string $field, int|string $value1, int|string $value2): static
    {
        return $this->between($field, $value1, $value2, 'NOT ');
    }

    /**
     * @param string $field
     * @param int|string $value1
     * @param int|string $value2
     *
     * @return static
     */
    public function orBetween(string $field, int|string $value1, int|string $value2): static
    {
        return $this->between($field, $value1, $value2, '', 'OR');
    }

    /**
     * @param string $field
     * @param int|string $value1
     * @param int|string $value2
     *
     * @return static
     */
    public function orNotBetween(string $field, int|string $value1, int|string $value2): static
    {
        return $this->between($field, $value1, $value2, 'NOT ', 'OR');
    }

    /**
     * @param string $field
     * @param string $data
     * @param string $type
     * @param string $andOr
     *
     * @return static
     */
    public function like(string $field, string $data, string $type = '', string $andOr = 'AND'): static
    {
        $like = $this->escape($data);
        $where = $field . ' ' . $type . 'LIKE ' . $like;

        if ($this->grouped) {
            $where = '(' . $where;
            $this->grouped = false;
        }

        $this->where = is_null($this->where)
            ? $where
            : $this->where . ' ' . $andOr . ' ' . $where;

        return $this;
    }

    /**
     * @param array $fields
     * @param string $data
     * @param string $andOr
     * @return $this
     */
    public function groupOrLike(array $fields, string $data, string $andOr = 'AND'):static
    {
        $wheres = []; $where = '';
        if(!empty($fields) && count($fields) > 0)
        {
            $like = $this->escape($data);
            foreach ($fields as $field)
            {
                $wheres[] = $field . ' LIKE ' . $like;
            }
            $where = implode(' OR ', $wheres);
        }
        if(!empty($where))
        {
            $where = '(' . $where . ')';
        }
        $this->where = is_null($this->where)
            ? $where
            : $this->where . ' ' . $andOr . ' ' . $where;
        return $this;
    }

    /**
     * @param string $field
     * @param string $data
     *
     * @return static
     */
    public function orLike(string $field, string $data): static
    {
        return $this->like($field, $data, '', 'OR');
    }

    /**
     * @param string $field
     * @param string $data
     *
     * @return static
     */
    public function notLike(string $field, string $data): static
    {
        return $this->like($field, $data, 'NOT ');
    }

    /**
     * @param string $field
     * @param string $data
     *
     * @return static
     */
    public function orNotLike(string $field, string $data): static
    {
        return $this->like($field, $data, 'NOT ', 'OR');
    }

    /**
     * @param int $limit
     * @param int|null $limitEnd
     *
     * @return static
     */
    public function limit(int $limit, int $limitEnd = null): static
    {
        $this->limit = !is_null($limitEnd)
            ? $limit . ', ' . $limitEnd
            : $limit;

        return $this;
    }

    /**
     * @param int $offset
     *
     * @return static
     */
    public function offset(int $offset): static
    {
        $this->offset = $offset;

        return $this;
    }

    /**
     * @param int $perPage
     * @param int $page
     *
     * @return static
     */
    public function pagination(int $perPage, int $page): static
    {
        $this->limit = $perPage;
        $this->offset = (($page > 0 ? $page : 1) - 1) * $perPage;

        return $this;
    }

    /**
     * @param string $orderBy
     * @param string|null $orderDir
     *
     * @return static
     */
    public function orderBy(string $orderBy, string $orderDir = null): static
    {
        if (!is_null($orderDir)) {
            $this->orderBy = $orderBy . ' ' . strtoupper($orderDir);
        } else {
            $this->orderBy = str_contains($orderBy, ' ') || strtolower($orderBy) === 'rand()'
                ? $orderBy
                : $orderBy . ' ASC';
        }

        return $this;
    }

    /**
     * @param array|string $groupBy
     *
     * @return static
     */
    public function groupBy(array|string $groupBy): static
    {
        $this->groupBy = is_array($groupBy) ? implode(', ', $groupBy) : $groupBy;

        return $this;
    }

    /**
     * @param string $field
     * @param array|string|null $operator
     * @param string|null $val
     *
     * @return static
     */
    public function having(string $field, array|string $operator = null, string $val = null): static
    {
        if (is_array($operator)) {
            $fields = explode('?', $field);
            $where = '';
            foreach ($fields as $key => $value) {
                if (!empty($value)) {
                    $where .= $value . (isset($operator[$key]) ? $this->escape($operator[$key]) : '');
                }
            }
            $this->having = $where;
        } elseif (!in_array($operator, $this->operators, true)) {
            $this->having = $field . ' > ' . $this->escape($operator);
        } else {
            $this->having = $field . ' ' . $operator . ' ' . $this->escape($val);
        }

        return $this;
    }

    /**
     * @return int
     */
    public function numRows(): int
    {
        return $this->numRows;
    }

    /**
     * @return int|null
     */
    public function insertId(): mixed
    {
        return $this->insertId;
    }

    /**
     * @throw PDOException
     */
    public function error(): void
    {
        if ($this->debug === true) {
            if (PHP_SAPI === 'cli') {
                die("Query: " . $this->query . PHP_EOL . "Error: " . $this->error . PHP_EOL);
            }

            $msg = '<h1>Database Error</h1>';
            $msg .= '<h4>Query: <em style="font-weight:normal;">"' . $this->query . '"</em></h4>';
            $msg .= '<h4>Error: <em style="font-weight:normal;">' . $this->error . '</em></h4>';
            die($msg);
        }

        throw new PDOException($this->error . '. (' . $this->query . ')');
    }

    /**
     * @param mixed|null $type
     * @param string|null $argument
     *
     * @return mixed
     * @throws JsonException
     */
    public function get(mixed $type = null, string $argument = null):mixed
    {
        $this->limit = 1;
        $query = $this->getAll(true);
        return $type === true ? $query : $this->query($query, false, $type, $argument);
    }

    /**
     * @param mixed|null $type
     * @param string|null $argument
     *
     * @return mixed
     * @throws JsonException
     */
    public function getAll(mixed $type = null, $argument = null):mixed
    {
        $query = 'SELECT ' . $this->select . ' FROM ' . $this->from;

        if (!is_null($this->join)) {
            $query .= $this->join;
        }

        if (!is_null($this->where)) {
            $query .= ' WHERE ' . $this->where;
        }

        if (!is_null($this->groupBy)) {
            $query .= ' GROUP BY ' . $this->groupBy;
        }

        if (!is_null($this->having)) {
            $query .= ' HAVING ' . $this->having;
        }

        if (!is_null($this->orderBy)) {
            $query .= ' ORDER BY ' . $this->orderBy;
        }

        if (!is_null($this->limit)) {
            $query .= ' LIMIT ' . $this->limit;
        }

        if (!is_null($this->offset)) {
            $query .= ' OFFSET ' . $this->offset;
        }

        return $type === true ? $query : $this->query($query, true, $type, $argument);
    }

    /**
     * @param array $data
     * @param bool $type
     *
     * @return mixed
     * @throws JsonException
     */
    public function insert(array $data, bool $type = false): mixed
    {
        $query = 'INSERT INTO ' . $this->from;

        $values = array_values($data);
        if (isset($values[0]) && is_array($values[0])) {
            $column = implode(', ', array_keys($values[0]));
            $query .= ' (' . $column . ') VALUES ';
            foreach ($values as $value) {
                $val = implode(', ', array_map([$this, 'escape'], $value));
                $query .= '(' . $val . '), ';
            }
            $query = trim($query, ', ');
        } else {
            $column = implode(', ', array_keys($data));
            $val = implode(', ', array_map([$this, 'escape'], $data));
            $query .= ' (' . $column . ') VALUES (' . $val . ')';
        }

        if ($type === true) {
            return $query;
        }
        if ($this->query($query, false)) {
            $this->insertId = $this->pdo->lastInsertId();
            return $this->insertId();
        }

        return false;
    }

    /**
     * @param array $data
     * @param bool $type
     * @param bool $field
     *
     * @return mixed|string
     * @throws JsonException
     */
    public function update(array $data, bool $type = false, bool $field = false):mixed
    {
        if(!is_null($this->join))
        {
            $query = 'UPDATE ' . $this->from . ' '  . $this->join . ' SET ';
        }
        else
        {
            $query = 'UPDATE ' . $this->from . ' SET ';
        }
        $values = [];

        foreach ($data as $column => $val) {
            if($field === false)
            {
                $values[] = $column . '=' . $this->escape($val);
            }
            else
            {
                $values[] = $column . '=' . $val;
            }
        }
        $query .= implode(',', $values);

        return $this->prepareWhere($query, $type);
    }

    /**
     * @param string $field
     * @param int $count
     * @param string|null $operator
     * @param bool $type
     * @return mixed
     * @throws JsonException
     */
    public function incrementField(string $field, int $count, string|null $operator = null, bool $type = false): mixed
    {
        $query = is_null($operator) ?
            'UPDATE ' . $this->from . ' SET `' . $field . '` = `' . $field . '`+' . $count
            : 'UPDATE ' . $this->from . ' SET `' . $field . '` = `' . $field . '`' . $operator . $count
        ;

        return $this->prepareWhere($query, $type);
    }

    /**
     * @param bool $type
     *
     * @return mixed|string
     * @throws JsonException
     */
    public function delete(bool $type = false):mixed
    {
        $query = 'DELETE FROM ' . $this->from;

        if (!is_null($this->where)) {
            $query .= ' WHERE ' . $this->where;
        }

        if (!is_null($this->orderBy)) {
            $query .= ' ORDER BY ' . $this->orderBy;
        }

        if (!is_null($this->limit)) {
            $query .= ' LIMIT ' . $this->limit;
        }

        if ($query === 'DELETE FROM ' . $this->from) {
            $query = 'TRUNCATE TABLE ' . $this->from;
        }

        return $type === true ? $query : $this->query($query, false);
    }

    /**
     * @return mixed
     * @throws JsonException
     */
    public function analyze(): mixed
    {
        return $this->query('ANALYZE TABLE ' . $this->from, false);
    }

    /**
     * @return mixed
     * @throws JsonException
     */
    public function check(): mixed
    {
        return $this->query('CHECK TABLE ' . $this->from, false);
    }

    /**
     * @return mixed
     * @throws JsonException
     */
    public function checksum(): mixed
    {
        return $this->query('CHECKSUM TABLE ' . $this->from, false);
    }

    /**
     * @return mixed
     * @throws JsonException
     */
    public function optimize(): mixed
    {
        return $this->query('OPTIMIZE TABLE ' . $this->from, false);
    }

    /**
     * @return mixed
     * @throws JsonException
     */
    public function repair(): mixed
    {
        return $this->query('REPAIR TABLE ' . $this->from, false);
    }

    /**
     * @return array|false
     */
    public function fields(): bool|array
    {
        return $this->pdo->query('DESCRIBE ' . $this->from)->fetchAll(PDO::FETCH_COLUMN);
    }

    /**
     * @param $field
     * @return bool|array
     */
    public function getFieldType($field): bool|array
    {
        $smt = $this->pdo->query("SHOW COLUMNS FROM " . $this->from ." WHERE Field = '" . $field . "'");
        return $smt->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * @param string $field
     * @param array $params Order of elements in an array: [new of string, old of string] AND necessarily: Where
     * @return bool
     */
    public function replace(string $field, array $params): bool
    {
        if(count($params) === 2){
            $query = 'Update ' . $this->from . ' SET `'.$field . '` = replace(`' . $field . '`';
            foreach ($params as $item)
            {
                $query .= "," .$this->escape($item);
            }
            $query .= ')';
            if (!is_null($this->where)) {
                $query .= ' WHERE ' . $this->where;
            }
            $smt = $this->pdo->prepare($query);
            $result = $smt->execute();
        }
        else
        {
            return false;
        }
        return $result;
    }

    /**
     * @param string $query String sql
     * @param bool $create
     * @param bool $assoc
     * @param bool $class
     * @return array|bool
     */
    public function onlyQuery(string $query, bool $create = false, bool $assoc = true, bool $class = false):array|bool
    {
        if(!empty($query))
        {
            $smt = $this->pdo->prepare($query);
            if($smt->execute())
            {
                if($create === false)
                {
                    if($class === false)
                    {
                        if($assoc === true)
                        {
                            $result = $smt->fetchAll(PDO::FETCH_ASSOC);/*FETCH_CLASS*/
                        }
                        else
                        {
                            $result = $smt->fetchAll(PDO::FETCH_NUM);
                        }
                    }
                    else
                    {
                        $result = $smt->fetchAll(PDO::FETCH_CLASS);
                    }
                }
                else
                {
                    $result = true;
                }
            }
            else
            {
                $result = false;
            }
            return $result;
        }
        return false;
    }

    /**
     * @return bool
     */
    public function transaction(): bool
    {
        if (!$this->transactionCount++) {
            return $this->pdo->beginTransaction();
        }

        $this->pdo->exec('SAVEPOINT trans' . $this->transactionCount);
        return $this->transactionCount >= 0;
    }

    /**
     * @return bool
     */
    public function commit(): bool
    {
        if (!--$this->transactionCount) {
            return $this->pdo->commit();
        }

        return $this->transactionCount >= 0;
    }

    /**
     * @return bool
     */
    public function rollBack(): bool
    {
        if (--$this->transactionCount) {
            $this->pdo->exec('ROLLBACK TO trans' . ($this->transactionCount + 1));
            return true;
        }

        return $this->pdo->rollBack();
    }

    /**
     * @return mixed
     */
    public function exec(): mixed
    {
        if (is_null($this->query)) {
            return null;
        }

        $query = $this->pdo->exec($this->query);
        if ($query === false) {
            $this->error = $this->pdo->errorInfo()[2];
            $this->error();
        }

        return $query;
    }

    /**
     * @param int|string|null $type
     * @param string|null $argument
     * @param bool $all
     *
     * @return mixed
     */
    public function fetch(int|string $type = null, string $argument = null, bool $all = false): mixed
    {
        if (is_null($this->query)) {
            return null;
        }

        $query = $this->pdo->query($this->query);
        if (!$query) {
            $this->error = $this->pdo->errorInfo()[2];
            $this->error();
        }

        $type = $this->getFetchType($type);
        if ($type === PDO::FETCH_CLASS) {
            $query->setFetchMode($type, $argument);
        } else {
            $query->setFetchMode($type);
        }

        $result = $all ? $query->fetchAll() : $query->fetch();
        $this->numRows = is_array($result) ? count($result) : 1;
        return $result;
    }

    /**
     * @param string|null $type
     * @param string|null $argument
     *
     * @return mixed
     */
    public function fetchAll(string $type = null, string $argument = null): mixed
    {
        return $this->fetch($type, $argument, true);
    }

    /**
     * @param string $query
     * @param bool|array $all
     * @param mixed $type
     * @param string|null $argument
     *
     * @return mixed
     * @throws JsonException
     */
    public function query(string $query, bool|array $all = true, mixed $type = null, string $argument = null): mixed
    {
        $this->reset();
        if (is_array($all) || func_num_args() === 1) {
            $params = explode('?', $query);
            $newQuery = '';
            foreach ($params as $key => $value) {
                if (!empty($value)) {
                    $newQuery .= $value . (isset($all[$key]) ? $this->escape($all[$key]) : '');
                }
            }
            $this->query = $newQuery;
            return $this;
        }
        $this->query = preg_replace('/\s\s+|\t\t+/', ' ', trim($query));
        $str = false;
        foreach (['select', 'optimize', 'check', 'repair', 'checksum', 'analyze'] as $value) {
            if (stripos($this->query, $value) === 0) {
                $str = true;
                break;
            }
        }
        $type = $this->getFetchType($type);
        $cache = false;
        if (!is_null($this->cache) && $type !== PDO::FETCH_CLASS) {
            $cache = $this->cache->getCache($this->query, $type === PDO::FETCH_ASSOC);
        }
        if (!$cache && $str) {
            $sql = $this->pdo->query($this->query);
            if ($sql) {
                $this->numRows = $sql->rowCount();
                if ($this->numRows > 0) {
                    if ($type === PDO::FETCH_CLASS) {
                        $sql->setFetchMode($type, $argument);
                    } else {
                        $sql->setFetchMode($type);
                    }
                    $this->result = $all ? $sql->fetchAll() : $sql->fetch();
                }

                if (!is_null($this->cache) && $type !== PDO::FETCH_CLASS) {
                    $this->cache->setCache($this->query, $this->result);
                }
                $this->cache = null;
            } else {
                $this->cache = null;
                $this->error = $this->pdo->errorInfo()[2];
                $this->error();
            }
        } elseif ((!$cache && !$str) || ($cache && !$str)) {
            $this->cache = null;
            $this->result = $this->pdo->exec($this->query);

            if ($this->result === false) {
                $this->error = $this->pdo->errorInfo()[2];
                $this->error();
            }
        } else {
            $this->cache = null;
            $this->result = $cache;
            if(is_array($this->result))
            {
                $this->numRows = count($this->result);
            }
            elseif ($this->result === '')
            {
                $this->numRows = 0;
            }
            else
            {
                $this->numRows = 1;
            }
        }
        $this->queryCount++;
        return $this->result;
    }

    /**
     * @param mixed $data
     *
     * @return string|int|float
     */
    public function escape(mixed $data): string|int|float
    {
        if($data === null)
        {
            $data = 'NULL';
        }
        elseif(is_numeric($data))
        {
            $data = !is_float($data) ? (int)$data : $data;
        }
        elseif(is_string($data))
        {
            $data = $this->pdo->quote($data);
        }
        return $data;
    }

    /**
     * @param $time
     *
     * @return static
     */
    public function cache($time):static
    {
        $this->cache = new Cache($this->cacheDir, $time);

        return $this;
    }

    /**
     * @return int
     */
    public function queryCount():int
    {
        return $this->queryCount;
    }

    /**
     * @return string
     */
    public function getQuery():string
    {
        return $this->query;
    }

    /**
     * @return void
     */
    public function __destruct()
    {
        $this->pdo = null;
    }

    /**
     * @return void
     */
    protected function reset():void
    {
        $this->select = '*';
        $this->from = null;
        $this->where = null;
        $this->limit = null;
        $this->offset = null;
        $this->orderBy = null;
        $this->groupBy = null;
        $this->having = null;
        $this->join = null;
        $this->grouped = false;
        $this->numRows = 0;
        $this->insertId = null;
        $this->query = null;
        $this->error = null;
        $this->result = [];
        $this->transactionCount = 0;
    }

    /**
     * @param mixed $type
     *
     * @return int
     */
    protected function getFetchType(mixed $type):int
    {
        if($type === 'class')
        {
            return  PDO::FETCH_CLASS;
        }
        return $type === 'array'
            ? PDO::FETCH_ASSOC
            : PDO::FETCH_OBJ;
        /*return $type === 'class'
            ? PDO::FETCH_CLASS
            : ($type === 'array'
                ? PDO::FETCH_ASSOC
                : PDO::FETCH_OBJ);*/
    }

    /**
     * Optimize Selected fields for the query
     *
     * @param string $fields
     *
     * @return void
     */
    private function optimizeSelect(string $fields):void
    {
        $this->select = $this->select === '*'
            ? $fields
            : $this->select . ', ' . $fields;
    }

    /**
     * @param string $query
     * @param bool $type
     * @return mixed|string
     * @throws JsonException
     */
    protected function prepareWhere(string $query, bool $type): mixed
    {
        if (!is_null($this->where)) {
            $query .= ' WHERE ' . $this->where;
        }

        if (!is_null($this->orderBy)) {
            $query .= ' ORDER BY ' . $this->orderBy;
        }

        if (!is_null($this->limit)) {
            $query .= ' LIMIT ' . $this->limit;
        }
        return $type === true ? $query : $this->query($query, false);
    }
}
