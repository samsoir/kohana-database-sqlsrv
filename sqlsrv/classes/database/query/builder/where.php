<?php defined('SYSPATH') or die('No direct script access.');
/**
 * Database query builder for WHERE statements.
 * 
 * @note       the limit() method has been deprecated as it is
 *             not supported by SQL Server
 *
 * @package    Kohana/Database
 * @category   Query
 * @author     Kohana Team
 * @copyright  (c) 2008-2009 Kohana Team
 * @license    http://kohanaphp.com/license
 */
abstract class Database_Query_Builder_Where extends Kohana_Database_Query_Builder_Where {

	/**
	 * Return up to "LIMIT ..." results
	 *
	 * @param   integer  maximum results to return
	 * @return  $this
	 * @deprecated
	 */
	public function limit($number)
	{
		// This is not supported by MS SQL Server, gracefully ignore this command
		return $this;
	}
}