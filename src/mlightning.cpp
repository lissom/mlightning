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


int main(int argc, char* argv[]) {
    int returnValue = 0;
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
       return 1;
    }

    std::cout << "init finished" << std::endl;

    //The actual loading
    try {
        loader::Loader loader(settings);
        try {
            loader.run();
        } catch (std::exception &e) {
            std::cerr << "Failure executing loader: " << e.what() << std::endl;
            returnValue = 1;
        }

    } catch (std::exception &e) {
        std::cerr << "Unable to initialize: " << e.what() << std::endl;
        return 1;
    }

    totalTimer.stop();

    long totalSeconds = totalTimer.seconds();

    std::cout << "\nTotal time: " << totalSeconds / 60 << "m" << totalSeconds % 60 << "s" << std::endl;

    return returnValue;
}
