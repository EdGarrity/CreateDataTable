#include "StockPrice.h"
#include <iostream>
#include <fstream>

namespace domain
{
	/* Constants */
	const std::string StockPrice::columnNamesFilename_("C:\\Temp\\PushP\\push-3.1.0\\test - Search\\columnNames.csv");

	StockPrice::StockPrice(
		unsigned int _id,
		const SQL_TIMESTAMP_STRUCT& _date,
		unsigned int _month,
		unsigned int _day,
		unsigned int _dayOfYear,
		unsigned int _week,
		unsigned int _weekDay,
		double _adjOpen,
		double _normalizedOpen,
		double _normalizedClose,
		double _normalizedHigh,
		double _normalizedLow,
		double _normalizedVolume)
		: id_(_id),
		date_(_date),
		adjOpen_(_adjOpen)
	{
		data_record_.push_back(_month);
		data_record_.push_back(_day);
		data_record_.push_back(_dayOfYear);
		data_record_.push_back(_week);
		data_record_.push_back(_weekDay);
		data_record_.push_back(_normalizedOpen);
		data_record_.push_back(_normalizedClose);
		data_record_.push_back(_normalizedHigh);
		data_record_.push_back(_normalizedLow);
		data_record_.push_back(_normalizedVolume);
	}

	int StockPrice::AddData( const double _data, const std::string _label )
	{
		// Save index of data column to text file
		std::ofstream ofs(columnNamesFilename_, std::ios::out | std::ios::app);
		ofs << data_record_.size() << "|" << _label << "\n";
		ofs.close();

		data_record_.push_back( _data );

		return data_record_.size() - 1;
	}

	int StockPrice::AddData(const double _data)
	{
		data_record_.push_back(_data);

		return data_record_.size() - 1;
	}

	double StockPrice::GetData( int column )
	{
		double value = 0.0;

		if (column >= 0)
		{
			column %= data_record_.size();

			value = data_record_[ column ];
		}

		return value;
	}

	void StockPrice::normalizeData( const int _index, const double _m, const double _b )
	{
		double x = _m * data_record_[ _index ] + _b;
		//data_record_[ _index ] = x;
	}

	unsigned long DateTime::Value()
	{
		return year_ * 10000 + month_ * 100 + day_;
	}
}
