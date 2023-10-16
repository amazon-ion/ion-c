#include "common.h"

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

namespace fs = std::filesystem;

size_t file_size(const char *path) {
   struct stat filestat;

   if (0 == stat(path, &filestat)) {
      return filestat.st_size;
   }

   return 0;
}

void read_file(FILE *file, uint8_t *buffer, size_t size) {
   size_t offset = 0;
   size_t read = 0;
   do {
      read = fread(buffer+offset, 1, size - offset, file);
      offset += read;
   } while (read != 0);
}

void DataLoader::load_data(char const *full_path) {
   FILE *f = nullptr;
   if (NULL != (f = fopen(full_path, "r"))) {
      int fd = fileno(f);
      struct stat filestat;

      if (0 == fstat(fd, &filestat)) {
         _buffer = new uint8_t[filestat.st_size];
         _length = filestat.st_size;
         read_file(f, _buffer, _length);
      } else {
         printf("ERROR: Unable to get file size.\n");
         exit(1);
      }

      fclose(f);
   } else {
      printf("ERROR: Unable to open %s\n", full_path);
      exit(1);
   }
}

DataLoader::DataLoader(char const *full_path) : _buffer(nullptr) {
   load_data(full_path);
}

DataLoader::DataLoader(char const *dataset, char const *suffix, Format format) : _buffer(nullptr) {
   // Path should be:  ./data/<dataset>/<dataset>.[min.]<suffix>
   fs::path datapath = fs::path("data") / fs::path(dataset) / fs::path(dataset);
   switch (format) {
      case Format::Pretty:
         datapath += std::string(".pretty");
         break;
      case Format::Compact:
         datapath += std::string(".min");
         break;
      case Format::Binary:
         break;
   }
   datapath += std::string(".") + suffix;
   load_data(datapath.c_str());
}

DataLoader::~DataLoader() {
   delete[] _buffer;
}
