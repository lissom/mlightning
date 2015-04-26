/*
 * streamwriter.cpp
 *
 *  Created on: Apr 17, 2015
 *      Author: charlie
 */

#include "stream_writer.h"

namespace loader {

    void writeToStream(std::ostream& out, const FileChunkHeader blockType,
            const SequenceId sequenceId, const Compression formatType, const mongo::BufBuilder& data) {
        std::string compressed;
        uint64_t size;
        switch(formatType) {
        case Compression::none :
            size = data.len();
            out << static_cast<std::underlying_type<FileChunkHeader>::type>(blockType)
                    << sequenceId << static_cast<std::underlying_type<Compression>::type>(formatType)
                    << size;
            out.write(data.buf(), data.len());
            break;
        case Compression::snappy :
            if (snappy::Compress(data.buf(), data.len(), &compressed) >
                size_t(data.len() / 9 * 10)) {
                writeToStream(out, blockType, sequenceId, Compression::none, data);
                break;
            }
            size = compressed.size();
            out << int8_t(blockType) << sequenceId << int8_t(formatType) << size
                    << compressed;
            break;
        default :
            throw std::range_error("Invalid compression type in writeToStream");
        }
    }

    bool readFromStream(std::istream& in, FileChunkHeader* blockType, SequenceId* sequenceId,
            std::vector<char>* data) {
        std::underlying_type<FileChunkHeader>::type fileChunkHeader;
        std::underlying_type<Compression>::type compressionType;
        in >> fileChunkHeader;
        *blockType = static_cast<FileChunkHeader>(fileChunkHeader);
        in >> *sequenceId;
        in >> compressionType;
        uint64_t size;
        in >> size;
        switch (static_cast<Compression>(compressionType)) {
        case Compression::none :
            data->reserve(size);
            in.read(&(*data)[0], size);
            break;
        case Compression::snappy :
            std::vector<char> raw;
            raw.reserve(size);
            if(!snappy::IsValidCompressedBuffer(&raw[0],size))
                throw std::logic_error("Invalid compression buffer with snappy");
            size_t uncompressdSize;
            snappy::GetUncompressedLength(&raw[0], size, &uncompressdSize);
            data->reserve(uncompressdSize);
            if(!snappy::RawUncompress(&raw[0], size, &(*data)[0]))
                throw std::logic_error("Unable to decompress block with snappy");
            break;
        }
        return !in.eof();
    }

} /* namespace loader */
