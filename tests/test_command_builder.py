# -*- coding: utf-8 -*-

import unittest
import os
import shutil
 
#a config object must be instantiated first for each entry point of the application
from mobyle.common.config import Config
config = Config( os.path.join(os.path.dirname(__file__), 'test.conf'))
from mobyle.common.connection import connection

from mobyle.common.users import User
from mobyle.common.project import Project
from mobyle.common.job import Status
from mobyle.common.job import CustomStatus
from mobyle.common.job import ProgramJob
from mobyle.common.service import *
from mobyle.common.type import *
from mobyle.common.error import InternalError, UserValueError
from mobyle.execution_engine.command_builder import CommandBuilder, BuildLogger


class TestCommandBuilder(unittest.TestCase):
    
    
    def setUp(self):
        self.test_dir = '/tmp/test_mob2_exec_engine'
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir, ignore_errors=True)

        os.makedirs(self.test_dir)
        self.cwd = os.getcwd()
        os.chdir(self.test_dir)
           
        connection.ProgramJob.collection.remove({})
        connection.User.collection.remove({})
        connection.Project.collection.remove({})
        
        self.user = connection.User()
        self.user['email'] = 'foo@bar.fr'
        self.user.save()
        
        self.project = connection.Project()
        self.project['owner'] = self.user['_id']
        self.project['name'] = 'MyProject'
        self.project.save()
        
        self.status = Status(Status.INIT)
        
        #build a test service
        self.program = connection.Program()
        self.program['name'] = 'echo'
        self.program['command'] = 'echo '
        self.inputs = InputParagraph()
        self.outputs = OutputParagraph()
        self.program['inputs'] = self.inputs
        self.program['outputs'] = self.outputs
        self.input_string = InputProgramParameter()
        self.input_string['name'] = 'string'
        self.input_string['argpos'] = 99
        self.input_string['format'] = '" " + value'
        input_string_type = StringType()
        self.input_string['type'] = input_string_type
        input_options = InputProgramParagraph()
        input_options['argpos'] = 2
        input_n = InputProgramParameter()
        # n has no argpos, its argpos will be 2
        input_n['type'] = BooleanType()
        input_n['name'] = 'n'
        input_n['format'] = '" -n " if value else ""'
        input_options['children'].append(input_n)
        # e has an argpos of 3
        self.input_e = InputProgramParameter()
        self.input_e['type'] = BooleanType()
        self.input_e['argpos'] = 3
        self.input_e['name'] = 'e'
        self.input_e['format'] = '" -e " if value else ""'
        self.input_e['precond'] = {'n': True}
        input_options['children'].append(self.input_e)
        self.program['inputs']['children'].append(self.input_string)
        self.program['inputs']['children'].append(input_options)
        output_stdout = OutputProgramParameter()
        output_stdout['name'] = 'stdout'
        output_stdout['output_type'] = u'stdout'
        output_stdout_type = FormattedType()
        output_stdout['type'] = output_stdout_type
        self.program['outputs']['children'].append(output_stdout)
        self.program.init_ancestors()
        self.program.save()
        
        
    def tearDown(self):
        os.chdir(self.cwd)
        shutil.rmtree(self.test_dir)
        
        
    def test_service_simple(self):
        job = connection.ProgramJob()
        job['status'] = self.status
        job['project'] = self.project.id
        job['service'] = self.program
        job.dir = self.test_dir
        job['inputs'] = {}
        parameter_values = {'string':'hello world'}
        job.process_inputs(parameter_values)
        job.save()
        
        cb = CommandBuilder(job)
        cl = cb.build_command()
        self.assertEqual(cl, 'echo hello world')
        
    def test_precond(self):
        #parameter with precond     
        job = connection.ProgramJob()
        job['status'] = self.status
        job['project'] = self.project.id
        job['service'] = self.program
        job.dir = self.test_dir
        job['inputs'] = {}
        parameter_values = {'string':'hello world',
                            'e': True}
        job.process_inputs(parameter_values)
        job.save()
        
        cb = CommandBuilder(job)
        cl = cb.build_command()
        self.assertEqual(cl, 'echo hello world')

        job['inputs'] = {}
        parameter_values = {'string':'hello world',
                            'e': True,
                            'n': True}
        job.process_inputs(parameter_values)
        job.save()
        
        cb = CommandBuilder(job)
        cl = cb.build_command()
        self.assertEqual(cl, 'echo -n -e hello world')
        
        #paragraph with preconds
        
    
    def test_replace(self):     
        job = connection.ProgramJob()
        job['status'] = self.status
        job['project'] = self.project.id
        job['service'] = self.program
        job.dir = self.test_dir
        job['inputs'] = {}
        parameter_values = {'string':'"hello world"'}
        job.process_inputs(parameter_values)
        job.save()
        cb = CommandBuilder(job)
        cl = cb.build_command()
        self.assertEqual(cl, 'echo \\"hello world\\"')
        
        job['inputs'] = {}
        parameter_values = {'string':'hello @world'}
        job.process_inputs(parameter_values)
        job.save()
        cb = CommandBuilder(job)
        cl = cb.build_command()
        self.assertEqual(cl, 'echo hello \@world')
        
        job['inputs'] = {}
        parameter_values = {'string':'_SQ_hello world_SQ_'}
        job.process_inputs(parameter_values)
        job.save()
        cb = CommandBuilder(job)
        cl = cb.build_command()
        self.assertEqual(cl, "echo \'hello world\'")
        
        job['inputs'] = {}
        parameter_values = {'string':'_DQ_hello world_DQ_'}
        job.process_inputs(parameter_values)
        job.save()
        cb = CommandBuilder(job)
        cl = cb.build_command()
        self.assertEqual(cl, 'echo \"hello world\"')
        
    
    def test_mandatory(self):
        #mandatory with value
        self.input_string['mandatory'] = True
        self.program.save()
        job = connection.ProgramJob()
        job['status'] = self.status
        job['project'] = self.project.id
        job['service'] = self.program
        job.dir = self.test_dir
        job['inputs'] = {}
        parameter_values = {'string':'hello world'}
        job.process_inputs(parameter_values)
        job.save()
        cb = CommandBuilder(job)
        cl = cb.check_mandatory()
        self.assertTrue(cl)
        
        #mandatory without value
        job['inputs'] = {}
        parameter_values = {}
        job.process_inputs(parameter_values)
        job.save()
        cb = CommandBuilder(job)
        with self.assertRaises(UserValueError) as context:
            cl = cb.check_mandatory()
            parameters = context.exception.parameters
            self.assertListEqual(['string'], [p.name for p in parameters])
        
        #mandatory without value and preconds True
        self.input_string['mandatory'] = None
        self.input_e['mandatory'] = True
        self.program.init_ancestors()
        self.program.save()
        job['inputs'] = {}
        parameter_values = {'string':'"hello world"', 'n':True}
        job.process_inputs(parameter_values)
        job.save()
        cb = CommandBuilder(job)
        with self.assertRaises(UserValueError) as context:
            cl = cb.check_mandatory()
        parameters = context.exception.parameters
        self.assertListEqual(['e'], [p.name for p in parameters])
        
        #mandatory without value but preconds False
        self.input_string['mandatory'] = None
        self.input_e['mandatory'] = True
        self.program.save()
        job['inputs'] = {}
        parameter_values = {'string':'"hello world"'}
        job.process_inputs(parameter_values)
        job.save()
        cb = CommandBuilder(job)
        self.assertTrue(cl)
        
    def test_parameter_before_cmd(self):
        input_first = InputProgramParameter()
        input_first['name'] = 'string'
        input_first['argpos'] = -10
        input_first['format'] = '"ln -s toto titi && "'
        input_first_type = StringType()
        input_first['type'] = input_first_type      
        self.program['inputs']['children'].append(input_first)
        self.program.init_ancestors()
        self.program.save()
        job = connection.ProgramJob()
        job['status'] = self.status
        job['project'] = self.project.id
        job['service'] = self.program
        job.dir = self.test_dir
        job['inputs'] = {}
        parameter_values = {'string':'hello world', 'e': True, 'n': True}
        job.process_inputs(parameter_values)
        job.save()
        cb = CommandBuilder(job)
        cl = cb.build_command()
        self.assertEqual(cl, "ln -s toto titi && echo -n -e hello world")
        
    def test_parameter_is_cmd(self):
        self.program['command'] = None
        p_cmd = InputProgramParameter()
        p_cmd['name'] = 'cmd'
        p_cmd['command'] = True
        p_cmd['format'] = '"echo "'
        p_cmd_type = StringType()
        p_cmd['type'] = p_cmd_type      
        self.program['inputs']['children'].append(p_cmd)
        self.program.init_ancestors()
        self.program.save()
        job = connection.ProgramJob()
        job['status'] = self.status
        job['project'] = self.project.id
        job['service'] = self.program
        job.dir = self.test_dir
        job['inputs'] = {}
        parameter_values = {'string':'hello world', 'e': True, 'n': True}
        job.process_inputs(parameter_values)
        job.save()
        cb = CommandBuilder(job)
        cl = cb.build_command()
        self.assertEqual(cl, 'echo -n -e hello world')
        
    def test_build_env(self):
        job = connection.ProgramJob()
        job['status'] = self.status
        job['project'] = self.project.id
        job['service'] = self.program
        job.dir = self.test_dir
        job['inputs'] = {}
        parameter_values = {'string':'hello world', 'e': True, 'n': True}
        job.process_inputs(parameter_values)
        job.save()
        cb = CommandBuilder(job)
        build_env = cb.build_env()
        self.assertDictEqual(build_env, {})
        
        env_send = {'VARIABLE': 'value',
                    'PATH': 'path/to/insert/first'}
        self.program['env'] = env_send
        self.program.save()
        build_env = cb.build_env()
        self.assertDictEqual(build_env, {'VARIABLE': 'value',
                                         'PATH' : '{0}:{1}'.format(env_send['PATH'], os.environ['PATH'])
                                         })
        
        
        
                
if __name__ == '__main__':
    unittest.main()
