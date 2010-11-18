<?php
// Check for windows before use
if (Kohana::$is_windows !== TRUE)
{
	throw new Sqlsrv_Exception('The Kohana Sqlsrv MS SQL Server native driver requires Windows at the moment. The Kohana team encourage Microsoft to support other platforms.');
}