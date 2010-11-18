<?php

class DatabaseSqlSrvTest extends PHPUnit_Framework_TestCase {

    static protected $_test_instance;

    public function setUp()
    {
        self::$_test_instance = Database::instance('custom', array(
            'type'        => 'sqlsrv',
            'connection'  => array(
                'Server'             => '(local)',
                'Database'           => 'kohana_mssql',
                'UID'                => 'sa',
                'PWD'                => 'groin99',
                'ConnectionPooling'  => FALSE,
                'CharacterSet'       => 'UTF-8'
            ),
            'caching'     => FALSE,
            'profiling'   => TRUE
        ));

        self::$_test_instance->connect();
        self::$_test_instance->query(
            Database::INSERT,
            "CREATE TABLE [dbo].[test](
                [id] [bigint] IDENTITY(3,1) NOT NULL,
                [name] [nvarchar](32) NOT NULL,
                [description] [nvarchar](255) NOT NULL,
             )"
        );


    }

    public function tearDown()
    {
        self::$_test_instance->query(Database::DELETE, "DROP TABLE [dbo].[test]");

        self::$_test_instance->disonnect();
    }

    public function testInsert()
    {
        $result = self::$_test_instance->query(Database::INSERT, "IINSERT INTO [dbo].[test] ([name], [description]) VALUES('hello', 'how are you doing')");
        
        $this->assertGreaterThanOrEqual($result[0], 1);
    }

    /**
     * @depends testInsert
     */
    public function testSelect()
    {
        $result = self::$_test_instance->query(Database::SELECT, "SELECT [name] FROM [dbo].[test] WHERE [name] = 'hello'")->as_array();

        $this->assertEquals($result[0]['name'], 'hello');
    }

    /**
     * @depends testSelect
     */
    public function testUpdate()
    {
        $affected = self::$_test_instance->query(Database::UPDATE , "UPDATE [dbo].[test] SET [name] = 'goodbye' WHERE [name] = 'hello'");

        $this->assertEquals($affected, 1);
    }

    /**
     * @depends testUpdate
     */
    public function testDelete()
    {
        $affected = self::$_test_instance->query(Database::DELETE, "DELETE FROM [dbo].[test] WHERE [name] = 'goodbye'");

        $this->assertEquals($affected, 1);
    }

    /**
     * $depends testDelete
     */
    public function testQueryBuilder()
    {
        $result = DB::insert('test')->columns(array('name', 'description'))->values(array('cat', 'super cat'))->execute(self::$_test_instance);
        $this->assertGreaterThanOrEqual($result[0], 1);

        $result = DB::select('test')->where('name', '=', 'cat')->execute(self::$_test_instance)->as_array();
        $this->assertEquals($result[0]['name'], 'cat');

        $affected = DB::update('test')->columns(array('description'))->values(array('turbo cat'))->execute();
        $this->assertEquals($affected, 1);

        $affected = DB::delete('test')->where('name', '=', 'cat')->execute();
        $this->assertEquals($affected, 1);
    }


}