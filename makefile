
LINK    = @echo Linking $@ && g++ 
GCC     = @echo Compiling $@ && g++ 
GC      = @echo Compiling $@ && gcc 
AR      = @echo Generating Static Library $@ && ar crv
#FLAGS   = -g -DDEBUG -W -Wall -fPIC
FLAGS   = -g
GCCFLAGS =  `pkg-config --cflags --libs glib-2.0 `
DEFINES = 
HEADER  = -I ./
LIBS    = -lglib -lpthread -lrocksdb -lrt -lz  -ldl
LINKFLAGS =

#HEADER += -I./

#LIBS    += -lrt
#LIBS    += -lglib 

OBJECT := decold.o cal.o recipe.o common.o queue.o sync_queue.o containerstore.o


BIN_PATH = ./

TARGET = decold

$(TARGET) : $(OBJECT) 
	$(LINK) $(FLAGS) $(LINKFLAGS) -o $@ $^ $(LIBS)

.cpp.o:
	$(GCC) -c $(HEADER) $(FLAGS) $(GCCFLAGS) -o $@ $<

.c.o:
	$(GC) -c $(HEADER) $(FLAGS) -g -o $@ $<

install: $(TARGET)
	cp $(TARGET) $(BIN_PATH)

clean:
	rm -rf $(TARGET) *.o *.so *.a


# ++++++++++++++++++++++++++++++++++++++
