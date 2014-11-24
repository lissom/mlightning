/*    Copyright Charlie Page 2014
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#include <iostream>
#include "loader.h"
#include "mongo_cxxdriver.h"
#include "program_options.h"
#include "tools.h"

/**
 * Main todo:
 * C allow self sharded collections again
 * # auth
 * # non-shoot out mode (continue on error)
 * # support mongoS string (or old style)
 * # compression
 * # disk queues
 * # _id optimizations (drop, insert at end point, etc)
 * # moving index fields forward
 * # transforms
 * # support M to N moves
 */
int main(int argc, char* argv[]) {
    int returnValue = EXIT_SUCCESS;
    tools::SimpleTimer<> totalTimer;
    //C++ Driver
    mongo::client::initialize();

    //Read settings, then hand those settings off to the loader.
    loader::Loader::Settings settings;
    loader::setProgramOptions(settings, argc, argv);
    try {
        settings.process();
    } catch (std::exception &e) {
       std::cerr << "Unable to process settings: " << e.what() << std::endl;
       return EXIT_FAILURE;
    }

    std::cout << "Settings loaded" << std::endl;

    //The actual loading
    try {
        loader::Loader loader(settings);
        try {
            loader.run();
        } catch (std::exception &e) {
            std::cerr << "Failure executing loader: " << e.what() << std::endl;
            returnValue = EXIT_FAILURE;
        }

    } catch (std::exception &e) {
        std::cerr << "Unable to initialize: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    totalTimer.stop();

    long totalSeconds = totalTimer.seconds();

    std::cout << "\nTotal time: " << totalSeconds / 60 << "m" << totalSeconds % 60 << "s" << std::endl;

    return returnValue;
}
