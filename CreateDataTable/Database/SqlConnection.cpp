#include <iostream>
#include <sstream>
#include <string>
#include <cfloat>

#include "SqlConnection.h"
#include "..\Utilities\MyException.h"

#define MAXBUFLEN   1024

namespace database
{
SqlConnection::SqlConnection()
{
	// Allocate environment handle
	retcode = SQLAllocHandle( SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv );

	// Set the ODBC version environment attribute
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
	{
		std::stringstream errorMessage;

		errorMessage << "Error AllocHandle " << retcode;

		throw MyException(errorMessage.str());
	}

	retcode = SQLSetEnvAttr( henv, SQL_ATTR_ODBC_VERSION, ( void* )SQL_OV_ODBC3, 0 );

	// Allocate connection handle
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
	{
		std::stringstream errorMessage;

		errorMessage << "Error SetEnv " << retcode;

		SQLFreeHandle(SQL_HANDLE_ENV, henv);
		henv = NULL;

		throw MyException(errorMessage);
	}

	// Set login timeout to 5 seconds
	retcode = SQLAllocHandle( SQL_HANDLE_DBC, henv, &hdbc );

	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
	{
		std::stringstream errorMessage;

		errorMessage << "Error AllocHDB " << retcode;

		SQLFreeHandle(SQL_HANDLE_ENV, henv);
		henv = NULL;

		throw MyException(errorMessage);
	}

	SQLSetConnectAttr( hdbc, SQL_LOGIN_TIMEOUT, ( SQLPOINTER )5, 0 );
				
	// Connect to data source
#ifdef UNICODE
//	SQLWCHAR ConnStrIn[MAXBUFLEN] = L"DRIVER={SQL Server Native Client 11.0};Address=192.168.1.42;UID=MyPushGP;PWD=MyPushGP;MARS_Connection=Yes;";
	SQLWCHAR ConnStrIn[MAXBUFLEN] = L"DRIVER={SQL Server Native Client 11.0};Address=192.168.1.174;UID=MyPushGP;PWD=MyPushGP;MARS_Connection=Yes;";
	SQLWCHAR ConnStrOut[MAXBUFLEN];
#else
	SQLCHAR ConnStrIn[MAXBUFLEN] = "DRIVER={SQL Server Native Client 11.0};Address=127.0.0.1;UID=MyPushGP;PWD=MyPushGP;MARS_Connection=Yes;";
	SQLCHAR ConnStrOut[MAXBUFLEN];
#endif
	SQLSMALLINT cbConnStrOut = 0;

	retcode = SQLDriverConnect(hdbc,      // Connection handle
						NULL,         // Window handle
						ConnStrIn,      // Input connect string
						SQL_NTS,         // Null-terminated string
						ConnStrOut,      // Address of output buffer
						MAXBUFLEN,      // Size of output buffer
						&cbConnStrOut,   // Address of output length
						SQL_DRIVER_NOPROMPT);

	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
	{
		std::stringstream errorMessage;

		errorMessage << "Error SQLConnect " << retcode << std::endl;

		SQLGetDiagRec(SQL_HANDLE_DBC, hdbc,1, V_OD_stat, &V_OD_err, V_OD_msg,100,&V_OD_mlen);
		errorMessage << V_OD_msg << " (" << V_OD_err << ")";
	
		SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
		SQLFreeHandle(SQL_HANDLE_ENV, henv);
		hdbc = NULL;
		henv = NULL;

		throw MyException(errorMessage);
	}
}

SqlConnection::~SqlConnection()
{
//	SQLFreeHandle( SQL_HANDLE_DBC, hstmt );
	SQLDisconnect( hdbc );
	SQLFreeHandle( SQL_HANDLE_DBC, hdbc );
	SQLFreeHandle( SQL_HANDLE_ENV, henv );
}

SQLHSTMT SqlConnection::SQLPrepare( char* _statementText )
{
	SQLHSTMT hstmt;

	retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
	{
		std::stringstream errorMessage;

		errorMessage << "Error SqlConnection::SQLPrepare( " << _statementText << ") SQLAllocHandle() " << retcode << std::endl;

		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
		errorMessage << V_OD_msg << " (" << V_OD_err << ")";

		throw MyException(errorMessage);
	}

#ifdef UNICODE
	retcode = ::SQLPrepare(hstmt, (SQLWCHAR *) _statementText, SQL_NTS);
#else
	retcode = ::SQLPrepare(hstmt, (SQLCHAR *)_statementText, SQL_NTS);
#endif
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
	{
		std::stringstream errorMessage;

		errorMessage << "Error SqlConnection::SQLPrepare( " << _statementText << ") " << retcode << std::endl;

		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
		errorMessage << V_OD_msg << " (" << V_OD_err << ")";

		throw MyException(errorMessage);
	}

	return hstmt;
}

SQLRETURN SqlConnection::SQLBindUnsignedIntegerInputParameter( SQLHSTMT hstmt, int _parameterNumber, unsigned int* _parameterValue)
{
	retcode = SQLBindParameter(hstmt, _parameterNumber, SQL_PARAM_INPUT, SQL_C_USHORT, SQL_INTEGER, 0, 0, _parameterValue, 0, NULL);
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
	{
		std::stringstream errorMessage;

		errorMessage << "Error SqlConnection::SQLBindUnsignedIntegerInputParameter (" << _parameterNumber << ", " << _parameterValue << ") " << retcode << std::endl;

		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt,1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
		errorMessage << V_OD_msg << " (" << V_OD_err << ")";

		throw MyException(errorMessage);
	}

	return retcode;
}

SQLRETURN SqlConnection::SQLBindUnsignedLongInputParameter( SQLHSTMT hstmt, int _parameterNumber, unsigned long* _parameterValue)
{
	retcode = SQLBindParameter(hstmt, _parameterNumber, SQL_PARAM_INPUT, SQL_C_ULONG, SQL_INTEGER, 0, 0, _parameterValue, 0, NULL);
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
	{
		std::stringstream errorMessage;

		errorMessage << "Error SqlConnection::SQLBindInputParameter (" << _parameterNumber << ", " << _parameterValue << ") " << retcode << std::endl;

		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt,1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
		errorMessage << V_OD_msg << " (" << V_OD_err << ")";

		throw MyException(errorMessage);
	}

	return retcode;
}

SQLRETURN SqlConnection::SQLBindDoubleInputParameter( SQLHSTMT hstmt, int _parameterNumber, double* _parameterValue)
{
	retcode = SQLBindParameter(hstmt, _parameterNumber, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, _parameterValue, 0, NULL);
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
	{
		std::stringstream errorMessage;

		errorMessage << "Error SqlConnection::SQLBindInputParameter (" << _parameterNumber << ", " << _parameterValue << ") " << retcode << std::endl;

		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt,1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
		errorMessage << V_OD_msg << " (" << V_OD_err << ")";

		throw MyException(errorMessage);
	}

	return retcode;
}

//SQLRETURN SqlConnection::SQLBindInputParameter( SQLHSTMT hstmt, int _parameterNumber, const std::string _parameterValue)
//{
//	char buffer[5000];
//	strncpy(buffer, _parameterValue.c_str(), 4999);
//	buffer[4999] = '\0';
//
//	return SQLBindInputParameter( hstmt, _parameterNumber, buffer);
//}

SQLRETURN SqlConnection::SQLBindCharInputParameter( SQLHSTMT hstmt, int _parameterNumber, const char* _parameterValue)
{
	retcode = SQLBindParameter(hstmt, _parameterNumber, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 5000, 0, (SQLCHAR *)_parameterValue, strlen(_parameterValue), NULL);
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
	{
		std::stringstream errorMessage;

		errorMessage << "Error SqlConnection::SQLBindInputParameter (" << _parameterNumber << ", " << _parameterValue << ") " << retcode << std::endl;

		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt,1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
		errorMessage << V_OD_msg << " (" << V_OD_err << ")";

		throw MyException(errorMessage);
	}

	return retcode;
}

//SQLRETURN SqlConnection::SQLBindOutputParameter( int _parameterNumber, unsigned long* _parameterValue)
//{
//	SQLINTEGER cb = sizeof (_parameterValue);
//
//	retcode = SQLBindParameter(hstmt, _parameterNumber, SQL_PARAM_OUTPUT, SQL_C_SLONG, SQL_INTEGER, 0, 0, &_parameterValue, 0, &cb);
//	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
//	{
//		std::cerr << "Error SqlConnection::SQLBindOutputParameter (" << _parameterNumber << ", " << _parameterValue << ") " << retcode << std::endl;
//
//		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt,1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
//		std::cerr << V_OD_msg << " (" << V_OD_err << ")" << std::endl;
//	}
//
//	return retcode;
//}

SQLRETURN SqlConnection::SQLExecute( SQLHSTMT hstmt )
{
	bool done = false;

	do
	{
		retcode = ::SQLExecute(hstmt);

		if ( retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO || retcode == SQL_NO_DATA )
			done = true;

		else
		{
			std::stringstream errorMessage;

			errorMessage << "Error SqlConnection::SQLExecute " << retcode << std::endl;

			SQLGetDiagRec(SQL_HANDLE_STMT, hstmt,1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
			errorMessage << V_OD_msg << " (" << V_OD_err << ")" << std::endl;
			errorMessage << V_OD_stat;
			errorMessage << std::endl;

			if (strcmp((char *)V_OD_stat, "22003") == 0)
			{
				std::cout << errorMessage.str();
				done = true;
			}

			else if (V_OD_err != 1205)
				throw MyException(errorMessage);
		}
	} while (!done);

	return retcode;
}

//SQLRETURN SqlConnection::SQLExecDirect( char* _statementText )
//{
//	retcode = ::SQLExecDirect(hstmt, ( SQLCHAR* ) _statementText, SQL_NTS);
//	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
//	{
//		std::cerr << "Error SqlConnection::SQLExecDirect( " << _statementText << ") " << retcode << std::endl;
//
//		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
//		std::cerr << V_OD_msg << " (" << V_OD_err << ")" << std::endl;
//	}
//
//	return retcode;
//}

//SQLRETURN SqlConnection::SQLExecDirectAndFetch( char* _statementText )
//{
//	retcode = ::SQLExecDirect(hstmt, ( SQLCHAR* ) _statementText, SQL_NTS);
//	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
//	{
//		std::cerr << "Error SqlConnection::SQLExecDirectAndFetch( " << _statementText << ") " << retcode << std::endl;
//
//		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
//		std::cerr << V_OD_msg << " (" << V_OD_err << ")" << std::endl;
//	}
//
//	retcode = SQLFetch(hstmt);
//	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
//	{
//		std::cerr << "Error SqlConnection::SQLExecDirectAndFetch SQLFetch() " << retcode << std::endl;
//
//		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
//		std::cerr << V_OD_msg << " (" << V_OD_err << ")" << std::endl;
//	}
//
//	return retcode;
//}

unsigned long SqlConnection::GetRowCount( char* tableName )
{
	char sqlStatement[500];
	SQLHSTMT hstmt;
	SQLUINTEGER buf;
	SQLLEN indicator;

	sprintf_s( sqlStatement, 500, "SELECT p.rows AS [Row Count] FROM sys.partitions p (NOLOCK) INNER JOIN sys.indexes i (NOLOCK) ON p.object_id = i.object_id AND p.index_id = i.index_id WHERE OBJECT_SCHEMA_NAME(p.object_id) != 'sys' AND OBJECT_NAME(p.object_id) = '%s'", tableName );

	retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
	{
		std::stringstream errorMessage;

		errorMessage << "Error SqlConnection::GetRowCount SQLAllocHandle() " << retcode;

		throw MyException(errorMessage);
	}

#ifdef UNICODE
	retcode = ::SQLExecDirect(hstmt, (SQLWCHAR*)sqlStatement, SQL_NTS);
#else
	retcode = ::SQLExecDirect(hstmt, ( SQLCHAR* ) sqlStatement, SQL_NTS);
#endif
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO && retcode != SQL_NO_DATA)
	{
		std::stringstream errorMessage;

		errorMessage << "Error GetRowCount  SQLExecDirect( " << std::string(sqlStatement) << ") " << retcode << std::endl;

		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
		errorMessage << V_OD_msg << " (" << V_OD_err << ")";

		SQLFreeHandle( SQL_HANDLE_STMT, hstmt );

		throw MyException(errorMessage);
	}

	retcode = SQLBindCol( hstmt, 1, SQL_C_ULONG, &buf, sizeof( buf ), &indicator );
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
	{
		std::stringstream errorMessage;

		errorMessage << "Error GetRowCount  SQLBindCol() " << retcode << std::endl;

		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
		errorMessage << V_OD_msg << " (" << V_OD_err << ")";

		SQLFreeHandle( SQL_HANDLE_STMT, hstmt );

		throw MyException(errorMessage);
	}

	retcode = SQLFetch(hstmt);
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO && retcode != SQL_NO_DATA )
	{
		std::stringstream errorMessage;

		errorMessage << "Error GetRowCount  SQLFetch() " << retcode << std::endl;

		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
		errorMessage << V_OD_msg << " (" << V_OD_err << ")";

		SQLFreeHandle( SQL_HANDLE_STMT, hstmt );

		throw MyException(errorMessage);
	}

	if (retcode == SQL_NO_DATA)
		buf = 0;

	SQLFreeHandle( SQL_HANDLE_STMT, hstmt );

	return buf;
}

#ifdef UNICODE
unsigned long SqlConnection::GetSlowRowCount( const std::wstring& _sqlStatement )
#else
unsigned long SqlConnection::GetSlowRowCount(const std::string& _sqlStatement)
#endif
{
#ifdef UNICODE
	std::wstring sqlStatement;
#else
	std::string sqlStatement;
#endif
	SQLHSTMT hstmt;
	SQLUINTEGER buf;
	SQLLEN indicator;

//	sprintf( sqlStatement, "SELECT COUNT(*) FROM (%s) X", _sqlStatement );
	sqlStatement += "SELECT COUNT(*) FROM (" + _sqlStatement + ") X";

	retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
	{
		std::stringstream errorMessage;

		errorMessage << "Error SqlConnection::GetRowCount SQLAllocHandle() " << retcode;

		throw MyException(errorMessage);
	}

#ifdef UNICODE
	retcode = ::SQLExecDirect(hstmt, (SQLWCHAR*)sqlStatement.c_str(), SQL_NTS);
#else
	retcode = ::SQLExecDirect(hstmt, ( SQLCHAR* ) sqlStatement.c_str(), SQL_NTS);
#endif
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO && retcode != SQL_NO_DATA)
	{
		std::stringstream errorMessage;

		errorMessage << "Error GetRowCount  SQLExecDirect( " << std::string(sqlStatement) << ") " << retcode << std::endl;

		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
		errorMessage << V_OD_msg << " (" << V_OD_err << ")";

		SQLFreeHandle( SQL_HANDLE_STMT, hstmt );

		throw MyException(errorMessage);
	}

	retcode = SQLBindCol( hstmt, 1, SQL_C_ULONG, &buf, sizeof( buf ), &indicator );
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
	{
		std::stringstream errorMessage;

		errorMessage << "Error GetRowCount  SQLBindCol() " << retcode << std::endl;

		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
		errorMessage << V_OD_msg << " (" << V_OD_err << ")";

		SQLFreeHandle( SQL_HANDLE_STMT, hstmt );

		throw MyException(errorMessage);
	}

	retcode = SQLFetch(hstmt);
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO && retcode != SQL_NO_DATA )
	{
		std::stringstream errorMessage;

		errorMessage << "Error GetRowCount  SQLFetch() " << retcode << std::endl;

		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
		errorMessage << V_OD_msg << " (" << V_OD_err << ")";

		SQLFreeHandle( SQL_HANDLE_STMT, hstmt );

		throw MyException(errorMessage);
	}

	if (retcode == SQL_NO_DATA)
		buf = 0;

	SQLFreeHandle( SQL_HANDLE_STMT, hstmt );

	return buf;
}

//void SqlConnection::GetTable( char* _tableName, ColInfoType& _colInfo, TableDataType& _tableData )
//{
//	SqlConnection::GetTable( _tableName, "", _colInfo, _tableData );
//}

void SqlConnection::GetTable( ColInfoType& _colInfo, TableData& _tableData, const std::string& _sqlStatement, const std::string& _orderByClause )
{
	TableData_IX nullTable;
	KeyList keyList;

	getTable( _colInfo, _tableData, nullTable, _sqlStatement, _orderByClause, false, keyList );
}

void SqlConnection::GetTable( ColInfoType& _colInfo, TableData_IX& _tableData, const std::string& _sqlStatement, const KeyList& _keyList, const std::string& _orderByClause = 0 )
{
	TableData nullTable;

	getTable( _colInfo, nullTable, _tableData, _sqlStatement, _orderByClause, true, _keyList );
}

#ifdef UNICODE
void SqlConnection::getTable(ColInfoType& _colInfo, TableData& _tableData, TableData_IX& _tableData_ix, const std::wstring& _sqlStatement, const std::wstring& _orderByClause, bool createIndex, const KeyList& _keyList)
#else
void SqlConnection::getTable( ColInfoType& _colInfo, TableData& _tableData, TableData_IX& _tableData_ix, const std::string& _sqlStatement, const std::string& _orderByClause, bool createIndex, const KeyList& _keyList )
#endif
{
#ifdef UNICODE
	std::wstring sqlStatement;
#else
	std::string sqlStatement;
#endif
	SQLHSTMT hstmt;
	SQLUINTEGER buf;
	SQLLEN indicator;
	SQLSMALLINT numColumns;
	int numRows;
	
	numRows = GetSlowRowCount(_sqlStatement);

	if (!createIndex)
		_tableData.resize(numRows);

	sqlStatement = _sqlStatement;

	if (_orderByClause.empty() == false)
		sqlStatement += " " + _orderByClause;

	std::cout << sqlStatement << std::endl;

	retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO )
	{
		std::stringstream errorMessage;

		errorMessage << "Error SqlConnection::GetTable SQLAllocHandle() " << retcode;

		throw MyException(errorMessage);
	}

#ifdef UNICODE
	retcode = ::SQLExecDirect(hstmt, (SQLWCHAR*)sqlStatement.c_str(), SQL_NTS);
#else
	retcode = ::SQLExecDirect(hstmt, ( SQLCHAR* ) sqlStatement.c_str(), SQL_NTS);
#endif
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO && retcode != SQL_NO_DATA)
	{
		std::stringstream errorMessage;

		errorMessage << "Error GetTable  SQLExecDirect( " << std::string(sqlStatement) << ") " << retcode << std::endl;

		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
		errorMessage << V_OD_msg << " (" << V_OD_err << ")";

		SQLFreeHandle( SQL_HANDLE_STMT, hstmt );

		throw MyException(errorMessage);
	}

	retcode = SQLNumResultCols(hstmt, &numColumns);
	if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
	{
		std::stringstream errorMessage;

		errorMessage << "Error GetTable  SQLNumResultCols( " << std::string(sqlStatement) << ") " << retcode << std::endl;

		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
		errorMessage << V_OD_msg << " (" << V_OD_err << ")";

		SQLFreeHandle( SQL_HANDLE_STMT, hstmt );

		throw MyException(errorMessage);
	}

	ColumnInfo colInfo;

	for (SQLSMALLINT i = 0; i < numColumns; i++)
	{
		SQLDescribeCol 
			(
				hstmt,
				i + 1,
				colInfo.ColumnName,
				sizeof (colInfo.ColumnName),
				&colInfo.ColumnNameLength,
				&colInfo.ColumnType,
				&colInfo.ColumnSize,
				&colInfo.DecimalDigits,
				&colInfo.Nullable
			);

		//convert to SQL_CHAR if necessary so SqlGetData knows how to process
		switch (colInfo.ColumnType)
		{
			case SQL_VARCHAR : colInfo.ColumnType = SQL_CHAR; break;
			default : break;
		}

#undef max;
#undef min;

		colInfo.Index = i;
		colInfo.Min = std::numeric_limits<double>::max();
		colInfo.Max = std::numeric_limits<double>::min();

		_colInfo[ (char *)colInfo.ColumnName ] = colInfo;
	}

	int indexRow = 0;
	char date[20];

	while (SQLFetch(hstmt) == SQL_SUCCESS)
	{
		Record record;

		for ( SQLSMALLINT indexColumn = 0; indexColumn < numColumns; indexColumn++ )
		{
			std::string columnName;

			for ( ColInfoType::iterator it = _colInfo.begin() ; it != _colInfo.end(); ++it)
			{
				if (it->second.Index == indexColumn)
				{
					columnName = it->first;
					break;
				}
			}

			record[ columnName ].type = _colInfo[ columnName ].ColumnType;

			switch (_colInfo[columnName].ColumnType)
			{
				case SQL_CHAR: 
					retcode = SQLGetData
					(
						hstmt,											//SQLHSTMT StatementHandle,
						indexColumn + 1,								//SQLUSMALLINT ColumnNumber, starting at 1
						_colInfo[ columnName ].ColumnType,				//SQLSMALLINT TargetType,
						&record[ columnName ].data.SqlChar,				//SQLPOINTER TargetValuePtr,
						sizeof(record[ columnName ].data.SqlChar),		//SQLINTEGER BufferLength,
						&record[ columnName ].length					//SQLINTEGER * StrLen_or_IndPtr
					);

					if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
					{
						std::stringstream errorMessage;

						errorMessage << "Error GetTable  SQLGetData( SQL_CHAR, " << indexRow << ", " << columnName << " ) " << retcode << std::endl;

						SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
						errorMessage << V_OD_msg << " (" << V_OD_err << ")";

						SQLFreeHandle( SQL_HANDLE_STMT, hstmt );

						throw MyException(errorMessage);
					}

					record[ columnName ].value = (char *)(record[ columnName ].data.SqlChar);

					_colInfo[ columnName ].Min = 0;
					_colInfo[ columnName ].Max = 0;

					break;

				case SQL_INTEGER: 
					retcode = SQLGetData
					(
						hstmt,											//SQLHSTMT StatementHandle,
						indexColumn + 1,								//SQLUSMALLINT ColumnNumber, starting at 1
						_colInfo[ columnName ].ColumnType,				//SQLSMALLINT TargetType,
						&record[ columnName ].data.SqlInteger,			//SQLPOINTER TargetValuePtr,
						sizeof(record[ columnName ].data.SqlInteger),	//SQLINTEGER BufferLength,
						&record[ columnName ].length					//SQLINTEGER * StrLen_or_IndPtr
					);

					if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
					{
						std::stringstream errorMessage;

						errorMessage << "Error GetTable  SQLGetData( SQL_INTEGER, " << indexRow << ", " << columnName << " ) " << retcode << std::endl;

						SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
						errorMessage << V_OD_msg << " (" << V_OD_err << ")";

						SQLFreeHandle( SQL_HANDLE_STMT, hstmt );

						throw MyException(errorMessage);
					}

					record[ columnName ].value = std::to_string( static_cast<long long>( record[ columnName ].data.SqlInteger ) );

					_colInfo[ columnName ].Min
						= _colInfo[ columnName ].Min < (double)record[ columnName ].data.SqlInteger
						? _colInfo[ columnName ].Min
						: (double)record[ columnName ].data.SqlInteger;

					_colInfo[ columnName ].Max
						= _colInfo[ columnName ].Max > (double)record[ columnName ].data.SqlInteger
						? _colInfo[ columnName ].Max 
						: (double)record[ columnName ].data.SqlInteger;

					break;

				case SQL_FLOAT: 
					retcode = SQLGetData
					(
						hstmt,											//SQLHSTMT StatementHandle,
						indexColumn + 1,								//SQLUSMALLINT ColumnNumber, starting at 1
						SQL_C_DOUBLE,									//SQLSMALLINT TargetType,
						&record[ columnName ].data.SqlFloat,			//SQLPOINTER TargetValuePtr,
						sizeof(record[ columnName ].data.SqlFloat),		//SQLINTEGER BufferLength,
						&record[ columnName ].length					//SQLINTEGER * StrLen_or_IndPtr
					);

					if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
					{
						std::stringstream errorMessage;

						errorMessage << "Error GetTable  SQLGetData( SQL_FLOAT, " << indexRow << ", " << columnName << " ) " << retcode << std::endl;

						SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
						errorMessage << V_OD_msg << " (" << V_OD_err << ")";

						SQLFreeHandle( SQL_HANDLE_STMT, hstmt );

						throw MyException(errorMessage);
					}

					record[ columnName ].value = std::to_string( static_cast<long double>( record[ columnName ].data.SqlFloat ) );

					_colInfo[ columnName ].Min
						= _colInfo[ columnName ].Min < record[ columnName ].data.SqlFloat
						? _colInfo[ columnName ].Min 
						: record[ columnName ].data.SqlFloat;

					_colInfo[ columnName ].Max
						= _colInfo[ columnName ].Max > record[ columnName ].data.SqlFloat
						? _colInfo[ columnName ].Max 
						: record[ columnName ].data.SqlFloat;

					break;

				case SQL_TYPE_TIMESTAMP: 
					retcode = SQLGetData
					(
						hstmt,											//SQLHSTMT StatementHandle,
						indexColumn + 1,								//SQLUSMALLINT ColumnNumber, starting at 1
						SQL_C_TYPE_TIMESTAMP,							//SQLSMALLINT TargetType,
						&record[ columnName ].data.SqlDateTime,			//SQLPOINTER TargetValuePtr,
						sizeof(record[ columnName ].data.SqlDateTime),	//SQLINTEGER BufferLength,
						&record[ columnName ].length					//SQLINTEGER * StrLen_or_IndPtr
					);

					if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
					{
						std::stringstream errorMessage;

						errorMessage << "Error GetTable  SQLGetData( SQL_TYPE_TIMESTAMP, " << indexRow << ", " << columnName << " ) " << retcode << std::endl;

						SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
						errorMessage << V_OD_msg << " (" << V_OD_err << ")";

						SQLFreeHandle( SQL_HANDLE_STMT, hstmt );

						throw MyException(errorMessage);
					}

					sprintf_s( date, 20, "%02d/%02d/%02d %02d:%02d:%02d", 
						record[ columnName ].data.SqlDateTime.day, 
						record[ columnName ].data.SqlDateTime.month, 
						record[ columnName ].data.SqlDateTime.year, 
						record[ columnName ].data.SqlDateTime.hour, 
						record[ columnName ].data.SqlDateTime.minute, 
						record[ columnName ].data.SqlDateTime.second );

					record[ columnName ].value = date;

					_colInfo[ columnName ].Min = 0;
					_colInfo[ columnName ].Max = 0;

					break;

				case SQL_BIGINT: //-5: // SQL_TYPE_BIGINT:
					retcode = SQLGetData
					(
						hstmt,											//SQLHSTMT StatementHandle,
						indexColumn + 1,								//SQLUSMALLINT ColumnNumber, starting at 1
						SQL_C_SBIGINT,									//SQLSMALLINT TargetType,
						&record[ columnName ].data.SqlBigInt,			//SQLPOINTER TargetValuePtr,
						sizeof(record[ columnName ].data.SqlBigInt),	//SQLINTEGER BufferLength,
						&record[ columnName ].length					//SQLINTEGER * StrLen_or_IndPtr
					);

					if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
					{
						std::stringstream errorMessage;

						errorMessage << "Error GetTable  SQLGetData( SQL_TYPE_BIGINT, " << indexRow << ", " << columnName << " ) " << retcode << std::endl;

						SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
						errorMessage << V_OD_msg << " (" << V_OD_err << ")";

						SQLFreeHandle( SQL_HANDLE_STMT, hstmt );

						throw MyException(errorMessage);
					}

					record[ columnName ].value = std::to_string( static_cast<long long>( record[ columnName ].data.SqlInteger ) );

					_colInfo[ columnName ].Min
						= _colInfo[ columnName ].Min < (double)record[ columnName ].data.SqlBigInt
						? _colInfo[ columnName ].Min
						: (double)record[ columnName ].data.SqlBigInt;

					_colInfo[ columnName ].Max
						= _colInfo[ columnName ].Max > (double)record[ columnName ].data.SqlBigInt
						? _colInfo[ columnName ].Max
						: (double)record[ columnName ].data.SqlBigInt;

					break;

				case SQL_BIT: //-7
					retcode = SQLGetData
					(
						hstmt,											//SQLHSTMT StatementHandle,
						indexColumn + 1,								//SQLUSMALLINT ColumnNumber, starting at 1
						_colInfo[ columnName ].ColumnType,				//SQLSMALLINT TargetType,
						&record[ columnName ].data.SqlBit,			    //SQLPOINTER TargetValuePtr,
						sizeof(record[ columnName ].data.SqlBit),	    //SQLINTEGER BufferLength,
						&record[ columnName ].length					//SQLINTEGER * StrLen_or_IndPtr
					);

					if ( retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
					{
						std::stringstream errorMessage;

						errorMessage << "Error GetTable  SQLGetData( SQL_INTEGER, " << indexRow << ", " << columnName << " ) " << retcode << std::endl;

						SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, V_OD_stat, &V_OD_err, V_OD_msg, 2000, &V_OD_mlen);
						errorMessage << V_OD_msg << " (" << V_OD_err << ")";

						SQLFreeHandle( SQL_HANDLE_STMT, hstmt );

						throw MyException(errorMessage);
					}

					record[ columnName ].value = record[ columnName ].data.SqlBit == 0 ? "F" : "T";

					_colInfo[ columnName ].Min = 0;
					_colInfo[ columnName ].Max = 0;

					break;

				default:
					std::stringstream errorMessage;

					errorMessage << "Error GetTable  unknown column type( " << _colInfo[ columnName ].ColumnType << " ) " << retcode << std::endl;

					SQLFreeHandle( SQL_HANDLE_STMT, hstmt );

					throw MyException(errorMessage);

					break;
			}
		} // for (SQLSMALLINT indexColumn=0; indexColumn < numColumns; indexColumn++)

		if (createIndex)
		{
			//IndexKey indexKey;

			//indexKey.StockPriceId = record[ "StockPriceId" ].data.SqlInteger;

//			PeriodKey periodKey;
			std::string periodKey;

			if (_keyList.size() > 0)
			{
				for (KeyList::const_iterator it = _keyList.begin() ; it != _keyList.end(); ++it)
//					periodKey.Key.push_back( record[ *it ].data.SqlInteger );
				{
//					periodKey += std::to_string(static_cast<long>(record[ *it ].data.SqlInteger));
//					periodKey += std::to_string( static_cast<long long>(record[ *it ].data.SqlInteger) );
					periodKey += record[ *it ].value + ",";
				}
			}

			else
//				periodKey.Key.push_back(0);
				periodKey = "0";

			//PeriodKeyToRecordMapType periodKeyToRecordMap = _tableData_ix[ record[ "StockPriceId" ].data.SqlInteger ];

			//periodKeyToRecordMap[ periodKey ] = record;

			//_tableData_ix[ record[ "StockPriceId" ].data.SqlInteger ] = periodKeyToRecordMap;


			_tableData_ix[ record[ "StockPriceId" ].data.SqlInteger ][ periodKey ] = record;
		}

		else
			_tableData[ indexRow ] = record;

		indexRow++;
	} // while (SQLFetch(hstmt) == SQL_SUCCESS)

	for ( ColInfoType::iterator it = _colInfo.begin() ; it != _colInfo.end(); ++it)
	{
		std::string columnName = it->first;

		_colInfo[ columnName ].m = -2.0 / (_colInfo[ columnName ].Min - _colInfo[ columnName ].Max);
		_colInfo[ columnName ].b = 1.0 - (_colInfo[ columnName ].Max * (-2.0 / (_colInfo[ columnName ].Min - _colInfo[ columnName ].Max)));
	}

	if (!createIndex)
		_tableData.resize(indexRow);

	SQLFreeHandle( SQL_HANDLE_STMT, hstmt );
}

} // namespace mypushgp