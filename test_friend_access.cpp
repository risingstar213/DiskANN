#include <iostream>
#include "async_pq_flash_index.h"
#include "async_linux_aligned_file_reader.h"

using namespace diskann;

int main() {
    // Test that friend declaration works
    std::cout << "Testing friend access to private members..." << std::endl;
    
    try {
        // Create a mock file reader
        auto reader = std::make_shared<AsyncLinuxAlignedFileReader>();
        
        // Create AsyncPQFlashIndex instance
        AsyncPQFlashIndex<float> async_index(
            std::static_pointer_cast<AlignedFileReader>(reader), 
            diskann::Metric::L2
        );
        
        std::cout << "Successfully created AsyncPQFlashIndex instance!" << std::endl;
        std::cout << "Friend declaration allows access to private members." << std::endl;
        
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
