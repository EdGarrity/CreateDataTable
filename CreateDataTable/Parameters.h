#pragma once

#include <string>

struct Parameters
{
	std::string archiveFilename;
	std::string sqlDateRangeFilter;
};

extern struct Parameters parameter;
