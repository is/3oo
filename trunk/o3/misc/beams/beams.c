#include <Python.h>

int
main(int argc, char *argv[])
{
	FILE *fp;
	Py_Initialize();
	PySys_SetArgv(argc, argv);
	
	fp = fopen(__BOOTNAME__, "r");
	PyRun_AnyFileEx(fp, __BOOTNAME__, 1);

	Py_Finalize();
	return 0;
}
