#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stdarg.h>

char* asnprintf(const char *arg, ...) {
    int err;
    char *str;
    va_list args;
    size_t size;
    
    size = strlen(arg) * 1.618;
    str = malloc(size);
    if(!str) return 0;
    
    
    va_start(args, arg);
    err = vsnprintf(str, size, arg, args);
    va_end(args);
    if(err < 0) {
        free(str);
        return 0;
    }
    
    if((unsigned)err >= size) {
        char *s;
        size = err+1;
        s = realloc(str, size);
        if(!s) {
            free(str);
            return 0;
        }
        str = s;
        
        va_start(args, arg);
        err = vsnprintf(str, size, arg, args);
        va_end(args);
        if(err < 0) {
            free(str);
            return 0;
        }
    }
    
    return str;
}

int main() {
    char *str;

    str = asnprintf("%lld", 5);
    free(str);
    
    str = asnprintf("%s", "This string is much longer and will require a realloc");
    free(str);

    return 0;
}
