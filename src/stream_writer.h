/*
 * streamwriter.h
 *
 *  Created on: Apr 17, 2015
 *      Author: charlie
 */

#pragma once

#include "loader_defs.h"
#include <ostream>
#include <istream>
#include <snappy.h>
#include <string>

namespace loader {
    void writeToStream(std::ostream& out, const FileChunkHeader blockType,
            const int32_t sequenceId, const Compression formatType, const mongo::BufBuilder& data);

    bool readFromStream(std::istream& in, int8_t* blockType, int32_t* sequenceId,
            std::vector<char>* data);

} /* namespace loader */
