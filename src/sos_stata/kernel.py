#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

import os
import shutil
import pandas as pd
import argparse
import tempfile
from sos.utils import env


class sos_stata:
    supported_kernels = {'Stata': ['stata']}
    background_color = '#CAE8F3'
    options = {}
    cd_command = "cd {dir}"

    def __init__(self, sos_kernel, kernel_name='stata'):
        self.sos_kernel = sos_kernel
        self.kernel_name = kernel_name
        self.init_statements = ''

    def get_vars(self, names):
        #
        # get variables with names from env.sos_dict and create
        # them in the subkernel. The current kernel should be stata
        temp_dir = tempfile.mkdtemp()
        try:
            for name in names:
                if not isinstance(env.sos_dict[name], pd.DataFrame):
                    if self.sos_kernel._debug_mode:
                        self.sos_kernel.warn('Cannot transfer a non DataFrame object {} of type {} to stata'.format(
                            name, env.sos_dict[name].__class__.__name__))
                    continue
                # convert dataframe to stata
                filename = os.path.join(temp_dir, f'{name}.dta')
                env.sos_dict[name].to_stata(filename)
                stata_code = f'use {filename}'
                self.sos_kernel.run_cell(stata_code, True, False, on_error='Failed to put variable {} to stata'.format(name))
        finally:
            # clear up the temp dir
            shutil.rmtree(temp_dir)

    def put_vars(self, items, to_kernel=None):
        # put stata dataset to Python as dataframe
        temp_dir = tempfile.mkdtemp()
        res = {}
        try:
            for idx, item in enumerate(items):
                try:
                    code = f'''\
local _olddir : pwd
cd {temp_dir}
save data_{idx}.dta
cd `_olddir'
'''
                    # run the code to save file
                    self.sos_kernel.run_cell(code, True, False, on_error=f"Failed to get data set {item} from stata")
                    # check if file exists
                    saved_file = os.path.join(temp_dir, f'data_{idx}.dta')
                    if not os.path.isfile(saved_file):
                        self.sos_kernel.warn('Failed to save dataset {} to {}'.format(item, saved_file))
                        continue
                    # now try to read it with Python
                    df = pd.read_stata(saved_file)
                    res[item] = df
                except Exception as e:
                    self.sos_kernel.warn('Failed to get dataset {} from stata: {}'.format(item, e))
        finally:
            # clear up the temp dir
            shutil.rmtree(temp_dir)
        return res

    def sessioninfo(self):
        # return information of the kernel
        stata_code = '''\
version
'''
        return self.sos_kernel.get_response(stata_code, ('stream',))[0][1]['text']
