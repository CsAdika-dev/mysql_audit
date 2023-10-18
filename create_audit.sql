DELIMITER $$

CREATE `create_audit`(IN `schemaName` varchar(255), IN `auditTable` varchar(255), IN `triggerSuffix` varchar(255))
BEGIN
    DECLARE tableName, columnName, triggerName VARCHAR(255) DEFAULT "";
    DECLARE triggerHandler, tableHandler, columnHandler INT DEFAULT 0;
    DROP TEMPORARY TABLE IF EXISTS ddlTable;
    CREATE TEMPORARY TABLE ddlTable (id INT NOT NULL AUTO_INCREMENT, command TEXT, PRIMARY KEY (id));
    INSERT INTO ddlTable (command) VALUES (CONCAT("USE `", schemaName,"`;

"));
    BlockDropTrigger: BEGIN
        DECLARE triggerCursor CURSOR FOR SELECT TRIGGER_NAME FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA = schemaName AND UPPER(TRIGGER_NAME) LIKE CONCAT("%_",triggerSuffix);
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET triggerHandler = 1;
        OPEN triggerCursor;
        triggerLoop: LOOP
            FETCH triggerCursor INTO triggerName;
            IF triggerHandler THEN
                LEAVE triggerLoop;
            END IF;
            SET @dropTriggerStatement = CONCAT("DROP TRIGGER `", triggerName, "`;");
            INSERT INTO ddlTable (command) VALUES (@dropTriggerStatement);
        END LOOP triggerLoop;
        CLOSE triggerCursor;
    END BlockDropTrigger;
    SET @createTable = CONCAT("CREATE TABLE IF NOT EXISTS `",auditTable,"` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `table_name` VARCHAR(255) NOT NULL,
  `old_row_data` JSON DEFAULT NULL,
  `new_row_data` JSON DEFAULT NULL,
  `dml_type` ENUM('INSERT','UPDATE','DELETE') NOT NULL,
  `dml_timestamp` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `dml_created_by` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

");
    INSERT INTO ddlTable (command) VALUES (@createTable);
    INSERT INTO ddlTable (command) VALUES ("DELIMITER $$

");
    BlockTable: BEGIN
        DECLARE tableCursor CURSOR FOR SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = schemaName AND TABLE_TYPE IN ("BASE TABLE") AND TABLE_NAME != AuditTable;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET tableHandler = 1;
        OPEN tableCursor;
        tableLoop: LOOP
            FETCH tableCursor INTO tableName;
            IF tableHandler THEN
                SET tableHandler = 0;
                LEAVE tableLoop;
            END IF;
            SET @insertStatementPrefix = CONCAT("CREATE TRIGGER `", tableName, "_insert_", triggerSuffix, "` AFTER INSERT ON `", tableName, "`
");
            SET @updateStatementPrefix = CONCAT("CREATE TRIGGER `", tableName, "_update_", triggerSuffix, "` AFTER UPDATE ON `", tableName, "`
");
            SET @deleteStatementPrefix = CONCAT("CREATE TRIGGER `", tableName, "_delete_", triggerSuffix, "` AFTER DELETE ON `", tableName, "`
");
            SET @statementPrefix = CONCAT("FOR EACH ROW
BEGIN
    INSERT INTO `",auditTable ,"` (
        `table_name`,
        `old_row_data`,
        `new_row_data`,
        `dml_type`,
        `dml_created_by`
    ) VALUES (
        '",tableName,"',");
            SET @insertStatement = CONCAT(@insertStatementPrefix,@statementPrefix,"
        NULL,
        JSON_OBJECT(
");
            SET @updateStatement = CONCAT(@updateStatementPrefix,@statementPrefix);
            SET @deleteStatement = CONCAT(@deleteStatementPrefix,@statementPrefix,"
        JSON_OBJECT(
");
            SET @updateStatementOld = "
        JSON_OBJECT(
";
            SET @updateStatementNew = "
        JSON_OBJECT(
";
            BlockColumn: BEGIN
                DECLARE columnCursor CURSOR FOR SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = schemaname and TABLE_NAME = tableName ORDER BY ORDINAL_POSITION;
                DECLARE CONTINUE HANDLER FOR NOT FOUND SET columnHandler = 1;
                OPEN columnCursor;
                columnLoop: LOOP
                    FETCH columnCursor INTO columnName;
                    IF columnHandler THEN
                        SET columnHandler = 0;
                        LEAVE columnLoop;
                    END IF;
                    SET @insertStatement = CONCAT(@insertStatement,"            \"",columnName,"\", NEW.",columnName,",
");
                    SET @updateStatementOld = CONCAT(@updateStatementOld,"            \"",columnName,"\", OLD.",columnName,",
");
                    SET @updateStatementNew = CONCAT(@updateStatementNew,"            \"",columnName,"\", NEW.",columnName,",
");
                    SET @deleteStatement = CONCAT(@deleteStatement,"            \"",columnName,"\", OLD.",columnName,",
");
--                    SELECT tableName, columnName;
                END LOOP columnLoop;
                CLOSE columnCursor;
            END BlockColumn;
            SET @statementSuffix = CONCAT("
        CURRENT_USER
    );
END$$
");
            SET @insertStatement = CONCAT(SUBSTRING(trim(@insertStatement),1,LENGTH(@insertStatement)-3),"),
        'INSERT',",@statementSuffix);
            SET @updateStatement = CONCAT(@updateStatement,SUBSTRING(TRIM(@updateStatementOld),1,LENGTH(@updateStatementOld)-3),"),",SUBSTRING(TRIM(@updateStatementNew),1,LENGTH(@updateStatementNew)-3),"),
        'UPDATE',",@statementSuffix);
            SET @deleteStatement = CONCAT(SUBSTRING(trim(@deleteStatement),1,LENGTH(@deleteStatement)-3),"),
        NULL,
        'DELETE',",@statementSuffix);
           INSERT INTO ddlTable (command) VALUES (@insertStatement);
           INSERT INTO ddlTable (command) VALUES (@updateStatement);
           INSERT INTO ddlTable (command) VALUES (@deleteStatement);
        END LOOP tableLoop;
        CLOSE tableCursor;
    END BlockTable;
    insert into ddlTable (command) values ("DELIMITER ;
");
    select group_concat(command separator "
") run_this_ddl from ddlTable order by id;
END$$

DELIMITER ;
