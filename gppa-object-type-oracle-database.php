/**
 * Gravity Perks // Populate Anything // Custom Oracle Database
 * https://gravitywiz.com/documentation/gravity-forms-populate-anything/
 *
 * === Created by: Bohdan Smaha ====
 *
 * ==== Requirements ====
 * - PHP OCI8 module installed https://www.php.net/manual/en/book.oci8.php
 * - Oracle client installed on the database server.
 * - TNSnames.ora with your DB Alias in the Oracle client network/admin directory.
 * 
 * By default, Populate Anything will only show the database that the current WordPress
 * installation is on.
 *
 * The following snippet allows registering an additional Oracle database with custom credentials.
 *
 * You can add multiple databases by adjusting the class names and adding additional
 * `gp_populate_anything()->register_object_type()` calls.
 */
add_action('init', function() {
    if (class_exists('GP_Populate_Anything')) {
        class GPPA_Object_Type_Oracle_Database extends GPPA_Object_Type {
			
			protected $_restricted = false;
			
			private static $blacklisted_columns = array('password', 'user_pass', 'user_activation_key');

			public $supports_null_filter_value = true;

			protected $db;
			private $tables_cache = array(); // Oracle tables format cache

			public function __construct($object_type) {
				parent::__construct($object_type);
				add_action(sprintf('gppa_pre_object_type_query_%s', $object_type), array($this, 'add_filter_hooks'));
			}

			public function add_filter_hooks() {
				add_filter(sprintf('gppa_object_type_%s_filter', $this->id), array($this, 'process_filter_default'), 10, 2);
			}

			public function get_object_id($object, $primary_property_value = null) {
				if (!$object || !$primary_property_value) {
					return null;
				}

				$props = array_keys($object);
				$key = $props[0];

				return $object[$key];
			}

			public function get_label() {
				return esc_html__('Oracle Database', 'gp-populate-anything');
			}

			public function get_groups() {
				return array(
					'columns' => array(
						'label' => esc_html__('Columns', 'gp-populate-anything'),
					),
				);
			}

			public function get_primary_property() {
				return array(
					'id'       => 'table',
					'label'    => esc_html__('Table', 'gp-populate-anything'),
					'callable' => array($this, 'get_tables'),
				);
			}

			public function get_properties($table = null) {
				if (!$table) {
					return array();
				}

				$properties = array();

				foreach ($this->get_columns($table) as $column) {
					$properties[$column] = array(
						'group'     => 'columns',
						'label'     => $column,
						'value'     => $column,
						'orderby'   => true,
						'callable'  => array($this, 'get_column_unique_values'),
						'args'      => array($table, $column),
						'operators' => array(
							'is',
							'isnot',
							'>',
							'>=',
							'<',
							'<=',
							'contains',
							'does_not_contain',
							'starts_with',
							'ends_with',
							'like',
							'is_in',
							'is_not_in',
						),
					);
				}

				return $properties;
			}

			public function get_db() {
				if (!isset($this->db)) {
					// These properties are assumed to be set in the extended class.
					$this->db = oci_connect($this->db_user, $this->db_password, $this->db_alias);
					if (!$this->db) {
						$e = oci_error();
						trigger_error(htmlentities($e['message'], ENT_QUOTES), E_USER_ERROR);
					}
				}
				return $this->db;
			}

			public function get_tables() {
				$db = $this->get_db();
				if (!$db) {
					return array();
				}

				$query = "SELECT table_name FROM all_tables";
				if ($this->schema) {
					$query .= " WHERE owner = '" . strtoupper($this->schema) . "'";
				}

				$stid = oci_parse($db, $query);
				if (!$stid) {
					$e = oci_error($db);
					return array();
				}

				if (!oci_execute($stid)) {
					$e = oci_error($stid);
					return array();
				}

				$tables = array();
				while (($row = oci_fetch_array($stid, OCI_ASSOC + OCI_RETURN_NULLS)) != false) {
					$tables[] = $row['TABLE_NAME'];
				}

				oci_free_statement($stid);

				return $tables;
			}

			public function get_columns($table) {
				$query = "SELECT column_name FROM all_tab_columns WHERE table_name = '" . strtoupper($table) . "'";
				if ($this->schema) {
					$query .= " AND owner = '" . strtoupper($this->schema) . "'";
				}

				$stid = oci_parse($this->get_db(), $query);
				if (!$stid) {
					$e = oci_error($this->get_db());
					return array();
				}
				if (!oci_execute($stid)) {
					$e = oci_error($stid);
					return array();
				}

				$columns = array();
				while (($row = oci_fetch_array($stid, OCI_ASSOC + OCI_RETURN_NULLS)) != false) {
					$columns[] = $row['COLUMN_NAME'];
				}

				oci_free_statement($stid);
				return $columns;
			}

			public function get_column_unique_values($table, $column) {
				$table = strtoupper($table);
				$col = strtoupper($column);

				$query = "SELECT DISTINCT " . $col . " FROM " . ($this->schema ? strtoupper($this->schema) . '.' : '') . $table . " WHERE ROWNUM <= " . gp_populate_anything()->get_query_limit($this);
				$query = apply_filters('gppa_object_type_oracle_database_column_value_query', $query, $table, $col, $this);

				$stid = oci_parse($this->get_db(), $query);
				if (!$stid) {
					$e = oci_error($this->get_db());
					return array();
				}
				if (!oci_execute($stid)) {
					$e = oci_error($stid);
					return array();
				}

				$values = array();
				while (($row = oci_fetch_array($stid, OCI_ASSOC + OCI_RETURN_NULLS)) != false) {
					$values[] = $row[$col];
				}

				oci_free_statement($stid);
				return $this->filter_values($values);
			}

			public function process_filter_default($query_builder_args, $args) {
				/** @var string|string[] */
				$filter_value = null;

				/** @var array */
				$filter = null;

				/** @var array */
				$filter_group = null;

				/** @var int */
				$filter_group_index = null;

				/** @var string */
				$primary_property_value = null;

				/** @var string */
				$property = null;

				/** @var string */
				$property_id = null;

				extract($args);

				if (is_array($filter) && isset($filter['operator']) && isset($filter_value)) {
					$query_builder_args['where'][$filter_group_index][] = array(
						'property' => $property_id,
						'operator' => $filter['operator'],
						'value' => $filter_value
					);
				}

				return $query_builder_args;
			}

			public function default_query_args($args) {
				/** @var string */
				$populate = null;

				/** @var array */
				$filter_groups = null;

				/** @var array */
				$ordering = null;

				/** @var array */
				$templates = null;

				/** @var string */
				$primary_property_value = null;

				/** @var array */
				$field_values = null;

				/** @var GF_Field */
				$field = null;

				/** @var boolean */
				$unique = null;

				/** @var int|null */
				$page = null;

				/** @var int */
				$limit = null;

				extract($args);

				$orderby = rgar($ordering, 'orderby');
				$order = rgar($ordering, 'order', 'ASC');

				return array(
					'select'   => '*',
					'from'     => $primary_property_value,
					'where'    => array(),
					'order_by' => $orderby,
					'order'    => $order,
				);
			}

			public function query_cache_hash($args) {
				$query_args = $this->process_query_args($args, $this->default_query_args($args));
				return $this->build_oracle_query(apply_filters('gppa_object_type_database_pre_query_parts', $query_args, $this), rgar($args, 'field'));
			}

			public function query($args) {
				$query_args = $this->process_query_args($args, $this->default_query_args($args));

				if (empty($query_args['from'])) {
					return array();
				}

				$query = $this->build_oracle_query($query_args, rgar($args, 'field'));

				// Debug constructed query
				// error_log("Executing Query: " . $query);

				$stid = oci_parse($this->get_db(), $query);
				if (!$stid) {
					$e = oci_error($this->get_db());
					// error_log("OCI Parse Error: " . htmlentities($e['message'], ENT_QUOTES));
					return array();
				}

				if (!oci_execute($stid)) {
					$e = oci_error($stid);
					// error_log("OCI Execute Error: " . htmlentities($e['message'], ENT_QUOTES));
					return array();
				}

				$objects = array();
				while (($row = oci_fetch_array($stid, OCI_ASSOC + OCI_RETURN_NULLS)) != false) {
					$objects[] = $row;
				}

				oci_free_statement($stid);

				// Debug fetched objects
				// error_log("Fetched Objects: " . print_r($objects, true));

				return $objects;
			}

			public function get_object_prop_value($object, $prop) {
				if (in_array($prop, self::$blacklisted_columns, true)) {
					return null;
				}

				if (!isset($object[$prop])) {
					return null;
				}

				return $object[$prop];
			}

			private function maybe_convert_to_date($table, $column, $value, $operator) {
				$is_date = false;

				if (isset($this->tables_cache[$table])) {
					$is_date = in_array($this->tables_cache[$table][$column], array('DATE', 'TIMESTAMP'), true);
				} else {
					$query = "SELECT column_name, data_type FROM user_tab_columns WHERE table_name = '" . strtoupper($table) . "'";
					$stid = oci_parse($this->get_db(), $query);
					oci_execute($stid);

					while (($row = oci_fetch_array($stid, OCI_ASSOC + OCI_RETURN_NULLS)) != false) {
						$this->tables_cache[$table][$row['COLUMN_NAME']] = $row['DATA_TYPE'];
						if ($row['COLUMN_NAME'] === $column && in_array($row['DATA_TYPE'], array('DATE', 'TIMESTAMP'), true)) {
							$is_date = true;
						}
					}

					oci_free_statement($stid);
				}

				if ($is_date) {
					$value = gmdate('Y-m-d', strtotime($value));

					switch ($operator) {
						case '>=':
						case '>':
							$value .= ' 00:00:00';
							break;

						case '<=':
						case '<':
							$value .= ' 23:59:59';
							break;
					}
				}

				return $value;
			}

			public function build_where_clause($table, $property, $operator, $value) {
				$property = self::esc_property_to_ident($property);

				// Handle special cases for Oracle
				switch ($operator) {
					case 'is':
						$operator = '=';
						break;
					case 'isnot':
						$operator = '!=';
						break;
					case 'contains':
						$operator = 'LIKE';
						$value = "'%" . str_replace("'", "''", $value) . "%'";
						break;
					case 'does_not_contain':
						$operator = 'NOT LIKE';
						$value = "'%" . str_replace("'", "''", $value) . "%'";
						break;
					case 'starts_with':
						$operator = 'LIKE';
						$value = "'" . str_replace("'", "''", $value) . "%'";
						break;
					case 'ends_with':
						$operator = 'LIKE';
						$value = "'%" . str_replace("'", "''", $value) . "'";
						break;
					case 'is_in':
						$operator = 'IN';
						$value = "(" . implode(", ", array_map(function($v) { return "'" . str_replace("'", "''", $v) . "'"; }, $value)) . ")";
						break;
					case 'is_not_in':
						$operator = 'NOT IN';
						$value = "(" . implode(", ", array_map(function($v) { return "'" . str_replace("'", "''", $v) . "'"; }, $value)) . ")";
						break;
				}

				// Properly format value for SQL
				if (is_string($value) && $operator !== 'LIKE' && $operator !== 'NOT LIKE' && $operator !== 'IN' && $operator !== 'NOT IN') {
					$value = "'" . str_replace("'", "''", $value) . "'";
				} elseif (is_array($value) && ($operator === 'IN' || $operator === 'NOT IN')) {
					$value = implode(", ", array_map(function($v) { return "'" . str_replace("'", "''", $v) . "'"; }, $value));
				}

				// Return the condition string
				return "{$property} {$operator} {$value}";
			}

			public function build_oracle_query($query_args, $field) {
				$query = array();

				// Build the SELECT clause
				$select = !is_array($query_args['select']) ? array($query_args['select']) : $query_args['select'];
				if ($select[0] === '*') {
					$select_clause = '*'; // Handle the case for selecting all columns
				} else {
					$select = array_map(array($this, 'esc_property_to_ident'), $select);
					$select_clause = implode(', ', $select);
				}

				// Build the FROM clause with schema
				$from = ($this->schema ? strtoupper($this->schema) . '.' : '') . self::esc_property_to_ident($query_args['from']);

				// Start constructing the query
				$query[] = "SELECT {$select_clause} FROM {$from}";

				// Add JOIN clauses if any
				if (!empty($query_args['joins'])) {
					foreach ($query_args['joins'] as $join_name => $join) {
						$query[] = $join;
					}
				}

				// Build the WHERE clauses
				$where_clauses = array();
				if (!empty($query_args['where'])) {
					foreach ($query_args['where'] as $where_or_grouping => $where_or_grouping_clauses) {
						$group_clauses = array();
						foreach ($where_or_grouping_clauses as $clause) {
							if (is_array($clause)) {
								$group_clauses[] = $this->build_where_clause($query_args['from'], $clause['property'], $clause['operator'], $clause['value']);
							} else {
								$group_clauses[] = $clause;
							}
						}
						$where_clauses[] = '(' . implode(' AND ', array_unique($group_clauses)) . ')';
					}

					if (!empty($where_clauses)) {
						$query[] = "WHERE " . implode(" OR ", $where_clauses);
					}
				}

				// Debug where clauses
				// error_log("Where Clauses: " . print_r($where_clauses, true));

				// Add GROUP BY clause if any
				if (!empty($query_args['group_by'])) {
					$group_by = self::esc_property_to_ident($query_args['group_by']);
					$query[] = "GROUP BY {$group_by}";
				}

				// Add ORDER BY clause if any
				if (!empty($query_args['order_by']) && !empty($query_args['order'])) {
					$order_by = self::esc_property_to_ident($query_args['order_by'], 'order_by');
					$order = $query_args['order'];

					if (!in_array(strtoupper($order), array('ASC', 'DESC', 'RAND'), true)) {
						$order = 'DESC';
					} elseif (strtoupper($order) === 'RAND') {
						$order_by = 'dbms_random.value';
						$order = '';
					}

					$query[] = "ORDER BY {$order_by} {$order}";
				}

				// Handle pagination with offset
				$offset = isset($query_args['offset']) ? $query_args['offset'] : null;
				if ($offset !== null) {
					$limit_query = "SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (";
					$limit_query .= implode("\n", $query);
					$limit_query .= ") a WHERE ROWNUM <= " . ($offset + $query_args['limit']) . ") WHERE rnum > " . $offset;
					$query = array($limit_query);
				} else {
					$query[] = "FETCH FIRST " . intval($query_args['limit']) . " ROWS ONLY";
				}

				$final_query = implode("\n", $query);

				// Debug final query
				// error_log("Final Query: " . $final_query);

				return $final_query;
			}

			public static function esc_property_to_ident($ident, $context = '') {
				return '"' . str_replace('"', '""', $ident) . '"';
			}

		}

        
        class GPPA_Object_Type_Oracle_Database_Custom extends GPPA_Object_Type_Oracle_Database {
            //Provide your DB credentials. Schema is optional.
            protected $db_user = 'YOUR_DB_USER';
            protected $db_password = 'YOUR_DB_USER_PASSWORD';
            protected $db_alias = 'YOUR_DB_ALIAS';
            protected $schema = 'YOUR_SCHEMA';

            public function get_label() {
                return esc_html__('Oracle Database', 'gp-populate-anything');
            }

            public function get_db() {
                if (!isset($this->db)) {
                    $this->db = oci_connect($this->db_user, $this->db_password, $this->db_alias);
                    if (!$this->db) {
                        $e = oci_error();
                        trigger_error(htmlentities($e['message'], ENT_QUOTES), E_USER_ERROR);
                    }
                }
                return $this->db;
            }
        }

        gp_populate_anything()->register_object_type('oracle-database', new GPPA_Object_Type_Oracle_Database_Custom('oracle-database'));
        error_log('Oracle Database object type registered');
    }
});
