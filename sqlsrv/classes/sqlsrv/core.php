<?php
/**
 * MS SQL Server native database connector (sqlsrv 2.0 driver).
 *
 * @package    Kohana/Database
 * @category   Drivers
 * @author     Kohana Team
 * @copyright  (c) 2008-2009 Kohana Team
 * @license    http://kohanaphp.com/license
 */
class Sqlsrv_Core extends Database {

	// Identifier for this connection within the PHP driver
	protected $_connection_id;

	// MSSQL uses a square brackets for identifiers
	protected $_identifier = array('[', ']');

	public function connect()
	{
		if ($this->_connection)
			return;

		// Extract the connection parameters, adding required variabels
		$mssql_config = $this->_config['connection'] + array(
			'Database'          => '',
			'Server'            => '',
			'UID'               => '',
			'PWD'               => '',
			'ConnectionPooling' => FALSE,
			'CharacterSet'      => 'UTF-8'
		);

		// Extract the Server name
		$server = $mssql_config['Server'];

		// Prevent this information from showing up in traces
		unset($mssql_config['Server'], $this->_config['connection']['UID'], $this->_config['connection']['PWD']);

        // If any illegal properties is in $mssql_config, sqlsrv_connect won't connect
        unset($mssql_config['hostname'], $mssql_config['database'], $mssql_config['username'], $mssql_config['password'], $mssql_config['persistent']);
        
		try
		{
			// Create a connection
			$this->_connection = sqlsrv_connect($server, $mssql_config);
			if ($this->_connection === FALSE)
				throw new Sqlsrv_Exception(':error, :sqlstate', array(
					':error'    => $errors[0]['message'],
					':sqlstate' => $errors[0]['SQLSTATE']
				), $errors[0]['code']);
		}
		catch (ErrorException $e)
		{
			$errors = sqlsrv_errors(SQLSRV_ERR_ALL);
			throw new Sqlsrv_Exception(':error, :sqlstate', array(
				':error'    => $errors[0]['message'],
				':sqlstate' => $errors[0]['SQLSTATE']
			), $errors[0]['code']);
		}

		// \xFF is a better delimiter, but the PHP driver uses underscore
		$this->_connection_id = sha1($server.'_'.$mssql_config['UID'].'_'.$mssql_config['PWD']);
	}

	public function disconnect()
	{
		try
		{
			// Database is assumed disconnected
			$status = TRUE;

			if (is_resource($this->_connection))
			{
				if ($status = sqlsrv_close($this->_connection))
				{
					// Clear the connection
					$this->_connection = NULL;
				}
			}
		}
		catch (Exception $e)
		{
			// Database is probably not disconnected
			$status = ! is_resource($this->_connection);
		}

		return $status;
	}

	public function set_charset($charset)
	{
		throw new Sqlsrv_Exception(':unsupported',
			array(':unsupported' => 'Setting of Character Set must be defined in the configuration during connection'
		));
	}

	public function query($type, $sql, $as_object = FALSE, array $params = NULL)
	{
		// Make sure the database is connected
		$this->_connection or $this->connect();

		if ( ! empty($this->_config['profiling']))
		{
			// Benchmark this query for the current instance
			$benchmark = Profiler::start("Database ({$this->_instance})", $sql);
		}

		// Detect type Database::INSERT and ensure last id is captured
		if ($type === Database::INSERT)
		{
			// We need to do some magic here to get the insert ID
			// this is a glorious hack!
			$sql_statement = (string) $sql;

			// Locate VALUES
			$values = strpos($sql, 'VALUES');

			$sql = substr($sql_statement, 0, $values).'output inserted.identitycol AS lastInsertId '.substr($sql_statement, $values);
		}

		// Execute the query
		if (($result = sqlsrv_query($this->_connection, $sql)) === FALSE)
		{
			// If something went wrong
			if (isset($benchmark))
			{
				// This benchmark is worthless
				Profiler::delete($benchmark);
			}

			// Get the errors
			$error = sqlsrv_errors(SQLSRV_ERR_ERRORS);

			// Throw an exception
			throw new Sqlsrv_Exception(':error [ :query ]',
				array(':error'  => $error[0]['message'], ':query' => $sql),
				$error[0]['code']
			);
		}

		if (isset($benchmark))
		{
			Profiler::stop($benchmark);
		}

		// Set the last query
		$this->last_query = $sql;

		if ($type === Database::SELECT)
		{
			// Return an iterator of results
			return new Database_Sqlsrv_Result($result, $sql, $as_object, $params);
		}
		elseif ($type === Database::INSERT)
		{
			// Get the last insert id
			if (($insert_id = sqlsrv_fetch_array($result)) === FALSE)
			{
				// Get the errors
				$error = sqlsrv_errors(SQLSRV_ERR_ERRORS);

				// Throw an exception
				throw new Sqlsrv_Exception(':error [ :query ]',
					array(':error'  => 'Unable to get the last inserted row ID from driver', ':query' => $sql),
					$error[0]['code']
				);
			}

			return array(
				$insert_id['lastInsertId'],
				sqlsrv_rows_affected($result),
			);
		}
		else
		{
			// Return the number of rows affected
			return sqlsrv_rows_affected($result);
		}
	}

	public function datatype($type)
	{
		static $types = array
		(
			'bigint'                    => array('type' => 'integer'),
			'binary'                    => array('type' => 'string', 'binary' => TRUE),
			'bit'                       => array('type' => 'int'),
			'char'                      => array('type' => 'string'),
			'date'                      => array('type' => 'datetime'),
			'datetime'                  => array('type' => 'datetime'),
			'datetime2'                 => array('type' => 'datetime'),
			'datetimeoffset'            => array('type' => 'datetime'),
			'decimal'                   => array('type' => 'float', 'exact' => TRUE),
			'float'                     => array('type' => 'float'),
			'geography'                 => array('type' => 'string', 'binary' => TRUE),
			'geometry'                  => array('type' => 'string', 'binary' => TRUE),
			'image'                     => array('type' => 'string', 'binary' => TRUE),
			'int'                       => array('type' => 'int'),
			'money'                     => array('type' => 'string'),
			'nchar'                     => array('type' => 'string'),
			'numeric'                   => array('type' => 'string'),
			'nvarchar'                  => array('type' => 'string'),
			'nvarchar(MAX)'             => array('type' => 'string', 'binary' => TRUE),
			'ntext'                     => array('type' => 'string', 'binary' => TRUE),
			'real'                      => array('type' => 'float', 'exact' => TRUE),
			'smalldatetime'             => array('type' => 'datetime'),
			'smallint'                  => array('type' => 'int'),
			'smallmoney'                => array('type' => 'string'),
			'sql_variant'               => array('type' => 'string'),
			'text'                      => array('type' => 'string', 'binary' => TRUE),
			'time'                      => array('type' => 'datetime'),
			'timestamp'                 => array('type' => 'string'),
			'tinyint'                   => array('type' => 'int'),
			'UDT'                       => array('type' => 'string', 'binary' => TRUE),
			'uniqueidentifier'          => array('type' => 'string'),
			'varbinary'                 => array('type' => 'string', 'binary' => TRUE),
			'varbinary(MAX)'            => array('type' => 'string', 'binary' => TRUE),
			'varchar'                   => array('type' => 'string'),
			'varchar(MAX)'              => array('type' => 'string', 'binary' => TRUE),
			'xml'                       => array('type' => 'string', 'binary' => TRUE),
		);

		$type = str_replace(array(' zerofill', ' identity'), '', $type);

		if (isset($types[$type]))
			return $types[$type];

		return parent::datatype($type);
	}

	public function list_tables($like = NULL)
	{
		if (is_string($like))
		{
			if ($query = sqlsrv_prepare($this->_connection, 'sp_tables @table_name=?', array($like)) === FALSE)
			{
				// Get the errors
				$error = sqlsrv_errors(SQLSRV_ERR_ERRORS);

				// Throw an exception
				throw new Sqlsrv_Exception(':error [ :query ]',
					array(':error'  => $error[0]['message'], ':query' => $sql),
					$error[0]['code']
				);
			}

			if (($result = sqlsrv_exec($query)) === FALSE)
			{
				// Get the errors
				$error = sqlsrv_errors(SQLSRV_ERR_ERRORS);

				// Throw an exception
				throw new Sqlsrv_Exception(':error [ :query ]',
					array(':error'  => $error[0]['message'], ':query' => $sql),
					$error[0]['code']
				);
			}
		}
		else
		{
			if ($result = sqlsrv_exec($this->_connection, 'sp_tables', array($like)))
			{
				// Get the errors
				$error = sqlsrv_errors(SQLSRV_ERR_ERRORS);

				// Throw an exception
				throw new Sqlsrv_Exception(':error [ :query ]',
					array(':error'  => $error[0]['message'], ':query' => $sql),
					$error[0]['code']
				);
			}
		}

		// Setup the return table and get the array
		$tables = array();
		$result_array = sqlsrv_fetch_array($result);

		// If the result an array
		if (is_array($result_array))
		{
			foreach ($result as $row)
			{
				$tables[] = reset($row);
			}
		}

		sqlsrv_free_stmt($result);
		return $tables;
	}

	public function list_columns($table, $like = NULL, $add_prefix = TRUE)
	{
		// Quote the table name
		$table = ($add_prefix === TRUE) ? $this->quote_table($table) : $table;

		if (is_string($like))
		{
			if ($query = sqlsrv_prepare($this->_connection, 'sp_columns @table_name=?', array($like)) === FALSE)
			{
				// Get the errors
				$error = sqlsrv_errors(SQLSRV_ERR_ERRORS);

				// Throw an exception
				throw new Sqlsrv_Exception(':error [ :query ]',
					array(':error'  => $error[0]['message'], ':query' => $sql),
					$error[0]['code']
				);
			}

			if (($result = sqlsrv_exec($query)) === FALSE)
			{
				// Get the errors
				$error = sqlsrv_errors(SQLSRV_ERR_ERRORS);

				// Throw an exception
				throw new Sqlsrv_Exception(':error [ :query ]',
					array(':error'  => $error[0]['message'], ':query' => $sql),
					$error[0]['code']
				);
			}
		}
		else
		{
			if ($result = sqlsrv_exec($this->_connection, 'sp_columns', array($like)))
			{
				// Get the errors
				$error = sqlsrv_errors(SQLSRV_ERR_ERRORS);

				// Throw an exception
				throw new Sqlsrv_Exception(':error [ :query ]',
					array(':error'  => $error[0]['message'], ':query' => $sql),
					$error[0]['code']
				);
			}
		}

		$columns = array();
		$result_array = sqlsrv_fetch_array($result);

		if (is_array($result_array))
		{
			foreach ($result_array as $row)
			{
				$column = $this->datatype($row['TYPE_NAME']);

				$column['column_name']      = $row['COLUMN_NAME'];
				$column['column_default']   = $row['COLUMN_DEF'];
				$column['data_type']        = $row['TYPE_NAME'];
				$column['is_nullable']      = ($row['NULLABLE'] == 1);
				$column['ordinal_position'] = $row['ORDINAL_POSITION'];

				switch ($column['type'])
				{
					case 'float':
						$column['numeric_precision'] = $row['PRECISION'];
						$column['numeric_scale'] = $row['SCALE'];
					break;
					case 'int':
						$column['display'] = $row['LENGTH'];
						$column['radix'] = $row['RADIX'];
					break;
					case 'string':
						switch ($column['data_type'])
						{
							case 'binary':
							case 'varbinary':
							case 'varbinary(MAX)':
							case 'varchar(MAX)':
							case 'geography':
							case 'geometry':
							case 'image':
							case 'char':
							case 'nchar':
							case 'nvarchar':
							case 'varchar':
							case 'money':
							case 'smallmoney':
							case 'numeric':
							case 'sql_variant':
							case 'uniqueidentifier':
							case 'decimal':
							case 'bigint':
								$column['character_maximum_length'] = $row['CHAR_OCTET_LENGTH'];
							break;
						}
					break;
				}

				// MySQL attributes
				$column['comment']      = $row['REMARKS'];   // NOT SUPPORTED AT THE MOMENT, WILL ALWAYS BE NULL
				$column['key']          = ((strpos(' identity') !== FALSE) ? 'identity' : FALSE);
				$columns[$row['Field']] = $column;
			}
		}

		sqlsrv_free_stmt($result);
		return $columns;
	}

	/**
	 * Quote a database identifier, such as a column name. Adds the
	 * table prefix to the identifier if a table name is present.
	 *
	 *     $column = $db->quote_identifier($column);
	 *
	 * You can also use SQL methods within identifiers.
	 *
	 *     // The value of "column" will be quoted
	 *     $column = $db->quote_identifier('COUNT("column")');
	 *
	 * Objects passed to this function will be converted to strings.
	 * [Database_Expression] objects will use the value of the expression.
	 * [Database_Query] objects will be compiled and converted to a sub-query.
	 * All other objects will be converted using the `__toString` method.
	 *
	 * @param   mixed   any identifier
	 * @return  string
	 * @uses    Database::table_prefix
	 */
	public function quote_identifier($value)
	{
		if ($value === '*')
		{
			return $value;
		}
		elseif (is_object($value))
		{
			if ($value instanceof Database_Query)
			{
				// Create a sub-query
				return '('.$value->compile($this).')';
			}
			elseif ($value instanceof Database_Expression)
			{
				// Use a raw expression
				return $value->value();
			}
			else
			{
				// Convert the object to a string
				return $this->quote_identifier((string) $value);
			}
		}
		elseif (is_array($value))
		{
			// Separate the column and alias
			list ($value, $alias) = $value;

			return $this->quote_identifier($value).' AS '.$this->quote_identifier($alias);
		}

		if (strpos($value, '"') !== FALSE)
		{
			// Quote the column in FUNC("ident") identifiers
			return preg_replace('/"(.+?)"/e', '$this->quote_identifier("$1")', $value);
		}
		elseif (strpos($value, '.') !== FALSE)
		{
			// Split the identifier into the individual parts
			$parts = explode('.', $value);

			if ($prefix = $this->table_prefix())
			{
				// Get the offset of the table name, 2nd-to-last part
				// This works for databases that can have 3 identifiers (Postgre)
				$offset = count($parts) - 2;

				// Add the table prefix to the table name
				$parts[$offset] = $prefix.$parts[$offset];
			}

			// Quote each of the parts
			return implode('.', array_map(array($this, __FUNCTION__), $parts));
		}
		else
		{
			return $this->_identifier[0].$value.$this->_identifier[1];
		}
	}

	/**
	 * 
	 *
	 * @todo FIX THIS, MAJOR SECURITY ISSUE!!!!!
	 */
	public function escape($value)
	{
		// SQL standard is to use single-quotes for all values
		return "'$value'";
	}

}