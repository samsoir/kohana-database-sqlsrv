<?php defined('SYSPATH') or die('No direct access allowed.');

return array
(
	'default' => array(
		'type'        => 'sqlsrv',
		'connection'  => array(
			'Server'             => '(local)',
			'Database'           => 'kohana',
			'UID'                => 'sa',
			'PWD'                => '',
			'ConnectionPooling'  => FALSE,
			'CharacterSet'       => 'UTF-8'
		),
		'caching'     => FALSE,
		'profiling'   => TRUE
	),
);