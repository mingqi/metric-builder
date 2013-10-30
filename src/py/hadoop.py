import subprocess, shutil, os, sys

from contextlib import closing
from glob import glob
from datetime import datetime, timedelta
from os.path import join, dirname, abspath, exists


class JobOnFS:

    def __init__(self, fs_root, start, end, java_path, jar_path, tmp_path):
        fmt = '%Y%m%d%H%M'
        self.fs_root = fs_root
        self.start = start
        self.end = end
        self.work_dir = "%s/%s_%s/" % (tmp_path, self.start.strftime(fmt), self.end.strftime(fmt))
        self.java_path = java_path
        self.jar_path = jar_path

    def _find_chunk_files(self):
        path_format = '%Y/%m/%d/%H/%M'
        file_format = '%Y%m%d%H%M'

        result = []
        for delta_minutes in range(0, (self.end - self.start).seconds / 60 ):
            d = self.start + timedelta(minutes=delta_minutes)
            path = d.strftime(path_format)
            file_prefix = d.strftime(file_format)
            result = result + glob(join(self.fs_root, path, file_prefix+'*'))

        return result


    def _copy_input(self, files):
        if exists(self.work_dir):
            shutil.rmtree(self.work_dir, True)

        os.makedirs(self.work_dir)

        for file in files:
            shutil.copy(file, self.work_dir)

        return self.work_dir


    def _make_output(self):
        output_dir = join(self.work_dir, 'output')
        if exists(output_dir):
            shutil.rmtree(output_dir, True)
        os.makedirs(output_dir)
        return output_dir


    def _find_output_files(self,output_dir):
        return glob(output_dir+"/part-r-*")

    def run_mapred(self):
        input_dir = self._copy_input(self._find_chunk_files())
        output_dir = self._make_output()
        print "input_dir: " + input_dir
        if subprocess.call([self.java_path, '-jar', self.jar_path, '--input', input_dir, '--output', output_dir]) != 0:
            sys.exit(1)

        result = []
        for output_file in self._find_output_files(output_dir
            ):
            with closing(open(output_file)) as out:
                for line in out:
                    result.append(line)

        return result



class JobOnHDFS:

    def __init__(self, hdfs_root, start, end):
        pass

    def run_mapred(self):
        pass

    def read_output(self):
        pass
