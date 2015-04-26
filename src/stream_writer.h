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
    using SequenceId = uint32_t;

    /**
     * Compresses the data in buffer and writes it to the steam
     */
    void writeToStream(std::ostream& out, const FileChunkHeader blockType,
            const SequenceId sequenceId, const Compression formatType, const mongo::BufBuilder& data);

    /**
     * Reads data from a stream into a data block.  The contents of data are destroyed
     * Returns true if there is more data to read
     */
    bool readFromStream(std::istream& in, FileChunkHeader* blockType, SequenceId* sequenceId,
            std::vector<char>* data);

} /* namespace loader */
