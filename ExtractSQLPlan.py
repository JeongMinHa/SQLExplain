
#-- oracledb
#  대용량 처리: 수백만 건의 데이터를 하나씩 읽어서 처리하거나(Generator), 메모리 사용량을 최소화해야 할 때.
#  단순 CRUD: 데이터를 한 두 건 조회하거나, 단순히 DB에 SELECT/INSERT/UPDATE만 하는 API를 만들 때.
#  속도가 최우선: 데이터 변환 과정 없이 DB와 통신만 할 때 가장 light.

#-- pandas
#  복잡한 가공: 그룹화(Groupby), 피벗 테이블, 여러 테이블 간의 복잡한 Join 작업을 파이썬에서 해야 할 때.
#  시각화/리포트: 엑셀 저장, CSV 변환, 차트 생성 등 데이터를 리포트 형태로 만들 때.
#  컬럼명 중심 작업: row[0]처럼 인덱스 단위의 아니라 df['USER_NAME']처럼 컬럼명으로 바로 접근하고 싶을 때.

# pip install oracledb
# pip install pyyaml

from datetime import datetime
import oracledb, yaml, re, time, logging

# 실행을 위한 기본 정보 로드
with open('../config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# DB Migration 시 Source 가 되는 DB
from_db_user    = config['from_database']['user']
from_db_pw      = config['from_database']['pw']
from_db_dsn     = config['from_database']['dsn']

# DB Migration 시 Target 이 되는 DB
to_db_user      = config['to_database']['user']
to_db_pw        = config['to_database']['pw']
to_db_dsn       = config['to_database']['dsn']

# DB Migration 시 Source 가 되는 계정 (스키마) 정보 나열
db_account_list = config['db_account_list']['user_list']
print("### CHECK USER(SCHEMA) ---> : ", db_account_list)

# 실행계획을 저장할 파일 위치
savefiledir     = config['fileinfo']['saved']

# 각 DB 연결을 위한 커넥션 선언
from_connection = None
to_connection   = None

try:
    from_connection = oracledb.connect(
        user        = from_db_user,
        password    = from_db_pw,
        dsn         = from_db_dsn
    )

    to_connection = oracledb.connect(
        user      = to_db_user,
        password  = to_db_pw,
        dsn       = to_db_dsn
    )

    # Source DB 에서 실행되는 SQL 을 추출하고자 함.
    # 추출 조건은 Oracle BackGround SQL 제외하고
    # 현재 실행되는 SQL 제외, 구분을 짓기 위해 PYTHON_SESSION_MON 이라는 코멘트 추가
    # SQL 처리 시, Unique 한 처리를 위해서 SQL_ID, CHILD_NUMBER 를 추출
    with (from_connection.cursor() as vsession_cursor):
        vsession_sql = ("SELECT /* PYTHON_SESSION_MON */"
                            "SQL_ID, "
                            "SQL_CHILD_NUMBER, "
                            "SCHEMANAME "
                        "FROM V$SESSION "
                        "WHERE TYPE <> 'BACKGROUND'"
                        "AND   SCHEMANAME IN ( :user_list ) "
                        "AND   SQL_ID IS NOT NULL")

        vsession_cursor.execute(vsession_sql, user_list = db_account_list)

        # 세션에서 추출된 리스트를 Loop 처리
        # 위에서 추출된 SQL의 SQL_ID, CHILD_NUMBER 를 기준으로 V$SQL 에서 SQL_FULL_TEXT 를 추출함.
        # 또한 V$SQL_BIND_CAPTURE 에서 값을 구분하기 위해, Address 정보 추출
        for session_row in vsession_cursor:
            print("### Session SQL_ID  --->", session_row[0])

            with from_connection.cursor() as vsql_cursor:
                vsql_sql = ("SELECT "
                                "RAWTOHEX(ADDRESS) AS ADDRESS, "
                                "CHILD_NUMBER, "
                                "SQL_FULLTEXT "
                            "FROM V$SQL "
                            "WHERE SQL_ID = :sql_id "
                            "AND   CHILD_NUMBER = :child_number "
                            "AND   SQL_TEXT NOT LIKE '%PYTHON_SESSION_MON%'"
                            "AND   ROWNUM = 1")

                # 데이터가 1GB 미만이라면, LOB 객체를 거치지 않고 바로 Python의 문자열(str)로 가져오는 것이 더 빠름.
                # 즉, fetch_lobs=False 옵션을 사용하면 드라이버가 내부적으로 최적화하여 데이터를 직접 가져옴.
                vsql_cursor.execute(vsql_sql, sql_id=session_row[0], child_number=session_row[1], fetch_lobs=False)

                clob_row = vsql_cursor.fetchone()

                if clob_row:
                    sql_full_text = clob_row[2]
                    print("### Session SQL Full Text ---> ", sql_full_text)

                    # 실행된 SQL 의 Bind Value 를 추출할 때 해당 값이 Varchar2 인지 Number 인지 구분하기 위해서, DATATYPE_STRING 을 추출함.
                    # 이 때 VARCHAR2(nn) 과 같이 추출되는 필요한 VARCHAR2 만 추출하기 위해서 CASE 구문을 사용했고, DB 에서 가져올 때 부터 값을 가공함.
                    with from_connection.cursor() as vbind_value_cursor:
                        vbind_value_sql = ("SELECT "
                                            "NAME, "
                                            "POSITION, "
                                            "CASE WHEN INSTR(DATATYPE_STRING, '(' ) > 0 "
                                            "    THEN SUBSTR(DATATYPE_STRING, 1, INSTR(DATATYPE_STRING, '(') -1) "
                                            "    ELSE DATATYPE_STRING "
                                            "END AS DATATYPE_STRING, "
                                            "VALUE_STRING "
                                           "FROM V$SQL_BIND_CAPTURE "
                                           "WHERE SQL_ID = :sql_id "
                                           "AND   ADDRESS = HEXTORAW( :address ) "
                                           "AND   CHILD_NUMBER = :child_number "
                                           "ORDER BY POSITION")

                        vbind_value_cursor.execute(vbind_value_sql, sql_id=session_row[0], address=clob_row[0], child_number=clob_row[1])

                        bind_value_rows = vbind_value_cursor.fetchall()

                        for name, position, datatype_string, value_string in bind_value_rows:

                            if value_string is None:
                                print("### Bind Value is not Exits ###")
                                continue

                            # Bind Value 매핑 시, VARCHAR2 의 경우 앞 뒤로 싱글 쿼테이션 추가
                            if 'NUMBER' in datatype_string:
                                value_string = float(value_string) if '.' in value_string else int(value_string)
                            elif 'DATE' in datatype_string or 'TIMESTAMP' in datatype_string:
                                value_string = value_string
                            elif 'VARCHAR2' in datatype_string:
                                value_string = f"'{value_string}'"

                            sql_full_text = sql_full_text.replace(name, value_string)

                        print("### Mapped SQL ---> ", sql_full_text)

                        # 각 SQL 의 실행계획을 추출 시, 구분하기 위해 statement_id 추가
                        statement_id = datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]
                        print("### Expalin Plan Statement ID ---> ", statement_id)

                        sql_full_text = "EXPLAIN PLAN SET STATEMENT_ID = '" + statement_id + "' FOR " + sql_full_text
                        print("### Full Mapped SQL ---> ", sql_full_text)

                        insert_sql = """
                            INSERT /* PYTHON_SESSION_MON */ INTO from_sql_list (from_list_seq, statement_id, sql)
                            VALUES (from_list_seq.NEXTVAL, :1, :2)
                        """
                        try:
                            from_insert_sql_cursor = from_connection.cursor()
                            insert_sql_values = (statement_id, sql_full_text)
                            from_insert_sql_cursor.execute(insert_sql, insert_sql_values)
                            from_connection.commit()
                            from_insert_sql_cursor.close()
                        except oracledb.Error as e:
                            print(f"Exception: {e}")

                        vbind_value_cursor.execute(sql_full_text)
                        vbind_value_cursor.execute("SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE', '" + statement_id + "', 'ALL'))")

                        # 실행계획을 파일로도 저장을 하지만,
                        # 실행계획에서 필요한 정보만 추출하기 위해 별도 변수를 생성.
                        from_explain_result = []

                        # 파일 생성 시, "a" 옵션은 append, "w" 새로 쓰기
                        for row in vbind_value_cursor.fetchall():
                            with open(savefiledir + "/" + statement_id + "_from_result.txt", "a", encoding="utf-8") as f:
                                print(row[0], file=f)
                                from_explain_result.append(row[0])

                        full_from_explain_result = "\n".join(from_explain_result)

                        # 필요한 데이터 추출을 위해 정규식 사용
                        # \n: 개행 문자(Newline), 즉 줄바꿈을 의미.
                        # \s: 공백 문자 (space, tab, newline, return, form feed, vertical tab).
                        # \s*: 화이트스페이스(공백, 탭, 줄바꿈 등)가 0개 이상 있음을 의미.
                        # *: 0번 이상 반복.
                        # \n: 다시 줄바꿈을 의미.
                        from_plan_text_parts = re.split(r'\n\s*\n', full_from_explain_result.strip())

                        # 위 결과에서 필요한 부분만 추출 (실행계획 테이블)
                        from_plan_table = from_plan_text_parts[1]

                        from_plan_lines = from_plan_table.strip().split('\n')

                        for line in from_plan_lines:

                            # "---" 가 포함되거나 "| Id" 가 포함된 라인은 Skip.
                            if '---' in line or '| Id' in line:
                                continue

                            words = line.split("|")

                            # Id Column
                            colId_value = words[1]
                            #print("### ID ---> " + colId_value)

                            # Operation Column
                            colOperation_value = words[2]
                            #print("### Operation ---> " + colOperation_value)

                            # Name Column
                            colName_value = words[3]
                            #print("### Name ---> " + colName_value)

                            # 위에서 추출된 Id, Operation, Name 정보를 DB 테이블에 저장하기 위한 작업
                            insert_explain_sql = """
                                INSERT /* PYTHON_SESSION_MON */ INTO from_sql_explain (from_sql_seq, statement_id, id, operation, name)
                                VALUES (from_sql_seq.NEXTVAL, :1, :2, :3, :4)
                            """

                            # insert_sql 에 데이터 매핑 및 실행
                            # 추가로 실행계획 시 추출되는 데이터가 많을 경우, executemany 메소드도 고려해 볼 수 있음.
                            try:
                                from_insert_explain_cursor = from_connection.cursor()
                                insert_explain_values = (statement_id, colId_value, colOperation_value, colName_value)
                                from_insert_explain_cursor.execute(insert_explain_sql, insert_explain_values)
                                from_connection.commit()

                                from_insert_explain_cursor.close()

                            except oracledb.Error as e:
                                print(f"Exception: {e}")

                        # TO DB 에 대한 실행계획 결과를 저장.
                        # 바로 위의 코드와 중복됨.
                        with (to_connection.cursor() as to_db_cursor):
                            to_db_cursor.execute(sql_full_text)
                            to_db_cursor.execute("SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE', '" + statement_id + "', 'ALL'))")

                            to_explain_result = []

                            for row in to_db_cursor.fetchall():
                                with open(savefiledir + "/" + statement_id + "_to_result.txt", "a", encoding="utf-8") as f:
                                    print(row[0], file=f)
                                    to_explain_result.append(row[0])

                            full_to_explain_result = "\n".join(to_explain_result)

                            to_plan_text_parts = re.split(r'\n\s*\n', full_to_explain_result.strip())

                            to_plan_table = to_plan_text_parts[1]

                            to_plan_lines = to_plan_table.strip().split('\n')

                            for line in to_plan_lines:
                                if '---' in line or '| Id' in line:
                                    continue

                                words = line.split("|")

                                colId_value = words[1]
                                colOperation_value = words[2]
                                colName_value = words[3]
                                insert_sql = """
                                    INSERT /* PYTHON_SESSION_MON */ INTO to_sql_explain (to_sql_seq, statement_id, id, operation, name)
                                    VALUES (to_sql_seq.NEXTVAL, :1, :2, :3, :4)
                                """

                                try:
                                    to_insert_explain_cursor = to_connection.cursor()
                                    insert_explain_values = (statement_id, colId_value, colOperation_value, colName_value)
                                    to_insert_explain_cursor.execute(insert_sql, insert_explain_values)
                                    to_connection.commit()

                                    to_insert_explain_cursor.close()

                                except oracledb.Error as e:
                                    print(f"Exception: {e}")

                else:
                    print('### Session SQL Is PYTHON_SESSION_MON')

except oracledb.Error as e:
    print(f"Exception : {e}")

finally:
    if 'from_connection' in locals() and from_connection:
        from_connection.close()
        print("### from_connection Closed.")

    if 'to_connection' in locals() and to_connection:
        to_connection.close()
        print("### to_connection Closed.")
