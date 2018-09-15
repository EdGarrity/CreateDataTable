#pragma once

#include <boost/serialization/vector.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/string.hpp>
#include <cstdarg>

#include "..\Database\SqlConnection.h"
#include "StockPrice.h"

namespace domain
{
	class Table
	{
	public:
		enum OrderType
		{
			Market = 0,
			Limit
		};

		Table();
		~Table();

		static StockPrice GetStockPrice(unsigned int index);

		/// <summary>Returns the data from the Stock Data Array pointed to by the row and column parameters</summary>
		/// <param name="row">Zero-based Row number index.  Must be greater than zero for derivative</param>  
		/// <param name="colum">Zero-based Column number index for stock data, negative column number for first derivative</param>  
		/// <returns>Stock data if column is less than or equal to 0, first derivative of stock data if column less than 0</returns>
		double GetStockData( unsigned int row, int column );

		static int GetNumberOfTrainingDataRows();
		std::string GetColumnName( int column );

		unsigned int GetIndicator( unsigned int _indicatorId, unsigned int _dataRowIndex );

		struct DataRange
		{
			long from;
			long to;
		};

		struct DataRange GetDataRange(const unsigned long from, const unsigned long to);
		unsigned long BinarySearchLeft(const unsigned long target);
		unsigned long BinarySearchRight(const unsigned long target);

		void ExportDataTable(const char* exportFileName, const unsigned long from, const unsigned long to);

	private:
		friend class boost::serialization::access;

		struct _ColumnInfo
		{
			int		index;
			double	min;
			double	max;
			double	m;
			double	b;

			template<class Archive>
			void serialize(Archive & ar, const unsigned int version)
			{
				ar & index;
				ar & min;
				ar & max;
				ar & m;
				ar & b;
			}
		};
		typedef struct _ColumnInfo ColumnInfoType;

		typedef std::map< std::string, _ColumnInfo > ColumnInfoMapType;

		//static const std::string archiveFilename_;
		static std::vector<StockPrice> trainingStockPrices_;
		static ColumnInfoMapType columnInfoMap_;

		void loadStockPrices();
		void loadTechnicalData( const std::string& _tableName, const std::string& _sqlStatement, std::vector<std::string> _keyField );

		bool loadStockDataFromArchive();
		bool saveStockDataToArchive();

		template<class Archive>
		void serialize(Archive & ar, const unsigned int version)
		{
			ar & columnInfoMap_;
			ar & trainingStockPrices_;
		}
	};
}

