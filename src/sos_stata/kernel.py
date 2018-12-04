#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

import os
import pandas as pd
from tempfile import TemporaryDirectory
from textwrap import dedent
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
        with TemporaryDirectory() as temp_dir:
            for name in names:
                if not isinstance(env.sos_dict[name], pd.DataFrame):
                    if self.sos_kernel._debug_mode:
                        self.sos_kernel.warn(f'Cannot transfer a non DataFrame object {name} to stata')
                    continue
                # convert dataframe to stata
                filename = os.path.join(temp_dir, f'{name}.dta')
                env.sos_dict[name].to_stata(filename)
                stata_code = f'use {filename}'
                self.sos_kernel.run_cell(stata_code, True, False,
                    on_error=f'Failed to put variable {name} to stata')

    def put_vars(self, items, to_kernel=None):
        # put stata dataset to Python as dataframe
        res = {}
        with TemporaryDirectory() as temp_dir:
            for idx, item in enumerate(items):
                try:
                    code = f'''\
                        local _olddir : pwd
                        cd {temp_dir}
                        save data_{idx}.dta
                        cd `_olddir'
                    '''
                    # run the code to save file
                    self.sos_kernel.run_cell(dedent(code), True, False,
                        on_error=f"Failed to get data set {item} from stata")
                    # check if file exists
                    saved_file = os.path.join(temp_dir, f'data_{idx}.dta')
                    if not os.path.isfile(saved_file):
                        self.sos_kernel.warn(f'Failed to save dataset {item} to {saved_file}')
                        continue
                    # now try to read it with Python
                    df = pd.read_stata(saved_file)
                    res[item] = df
                except Exception as e:
                    self.sos_kernel.warn(f'Failed to get dataset {item} from stata: {e}')
        return res

    def sessioninfo(self):
        # return information of the kernel
        return self.sos_kernel.get_response('version', ('stream',))[0][1]['text']
