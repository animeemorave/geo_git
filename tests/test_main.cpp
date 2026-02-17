#include <iostream>
#include <cstdlib>

extern void test_cas_basic();
extern void test_cas_hash_computation();
extern void test_cas_store_retrieve();
extern void test_cas_deduplication();

int main() {
    std::cout << "Running GeoVersion Control System tests..." << std::endl;
    
    test_cas_basic();
    test_cas_hash_computation();
    test_cas_store_retrieve();
    test_cas_deduplication();
    
    std::cout << "All tests completed." << std::endl;
    return EXIT_SUCCESS;
}
