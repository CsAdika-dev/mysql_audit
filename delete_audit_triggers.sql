DELIMITER $$

CREATE PROCEDURE `delete_audit_triggers`(IN `schemaName` VARCHAR(255), IN `triggerSuffix` VARCHAR(255))
BEGIN
    DECLARE tableName, columnName, triggerName VARCHAR(255) DEFAULT "";
    DECLARE triggerHandler, tableHandler, columnHandler INT DEFAULT 0;
    DROP TEMPORARY TABLE IF EXISTS ddlTable;
    CREATE TEMPORARY TABLE ddlTable (id INT NOT NULL AUTO_INCREMENT, command TEXT, PRIMARY KEY (id));
    INSERT INTO ddlTable (command) VALUES (CONCAT("USE ", schemaName, ";"));
    BlockDropTrigger: BEGIN
        DECLARE triggerCursor CURSOR FOR SELECT TRIGGER_NAME FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA = schemaName AND UPPER(TRIGGER_NAME) LIKE CONCAT("%_", triggerSuffix);
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET triggerHandler = 1;
        OPEN triggerCursor;
        triggerLoop: LOOP
            FETCH triggerCursor INTO triggerName;
            IF triggerHandler THEN
                LEAVE triggerLoop;
            END IF;
            SET @dropTriggerStatement = CONCAT(@dropTriggerStatement,"DROP TRIGGER ", triggerName, ";");
            INSERT INTO ddlTable (command) VALUES (@dropTriggerStatement);
        END LOOP triggerLoop;
        CLOSE triggerCursor;
    END BlockDropTrigger;
    SELECT GROUP_CONCAT(command SEPARATOR "\n") run_this_ddl FROM ddlTable ORDER BY id;
END$$

DELIMITER ;
