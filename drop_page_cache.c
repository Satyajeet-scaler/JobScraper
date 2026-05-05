#include <unistd.h>
#include <stdio.h>

int main(void) {
    sync();
    FILE *f = fopen("/proc/sys/vm/drop_caches", "w");
    if (!f) {
        return 1;
    }
    fprintf(f, "1\n");
    fclose(f);
    return 0;
}
