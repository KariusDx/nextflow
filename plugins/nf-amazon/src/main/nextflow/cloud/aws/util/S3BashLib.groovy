/*
 * Copyright 2013-2023, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.cloud.aws.util

import com.amazonaws.services.s3.model.CannedAccessControlList
import nextflow.Global
import nextflow.Session
import nextflow.cloud.aws.batch.AwsOptions
import nextflow.executor.BashFunLib

/**
 * AWS S3 helper class
 */
class S3BashLib extends BashFunLib<S3BashLib> {

    private String storageClass = 'STANDARD'
    private String storageEncryption = ''
    private String storageKmsKeyId = ''
    private String debug = ''
    private String s3Cli
    private String defaultS3Cli
    private String retryMode
    private String acl = ''

    S3BashLib withS3CliPath(String cliPath) {
        if( cliPath )
            this.s3Cli = cliPath
        return this
    }

    S3BashLib withDefaultS3Cli(String defaultS3Cli) {
        if( defaultS3Cli )
            this.defaultS3Cli = defaultS3Cli
        return this
    }

    S3BashLib withRetryMode(String value) {
        if( value )
            retryMode = value
        return this
    }

    S3BashLib withDebug(Boolean  value) {
        this.debug = value ? '--debug ' : ''
        return this
    }

    S3BashLib withStorageClass(String value) {
        if( value )
            this.storageClass = value
        return this
    }

    S3BashLib withStorageEncryption(String value) {
        if( value )
            this.storageEncryption = value ? "--sse $value " : ''
        return this
    }

    S3BashLib withStorageKmsKeyId(String value) {
        if( value )
            this.storageKmsKeyId = value ? "--sse-kms-key-id $value " : ''
        return this
    }

    S3BashLib withAcl(CannedAccessControlList value) {
        if( value )
            this.acl = "--acl $value "
        return this
    }

    protected String retryEnv() {
        if( !retryMode )
            return ''
        """
        # aws cli retry config
        export AWS_RETRY_MODE=${retryMode} 
        export AWS_MAX_ATTEMPTS=${maxTransferAttempts}
        """.stripIndent().rightTrim()
    }

    protected String s3Lib() {
        """
        # aws helper
        nxf_s3_upload() {
            local name=\$1
            local s3path=\$2
            if [[ "\$name" == - ]]; then
              $s3Cli cp --only-show-errors ${debug}${acl}${storageEncryption}${storageKmsKeyId}--storage-class $storageClass - "\$s3path"
            elif [[ -d "\$name" ]]; then
              $s3Cli cp --only-show-errors --recursive ${debug}${acl}${storageEncryption}${storageKmsKeyId}--storage-class $storageClass "\$name" "\$s3path/\$name"
            else
              $s3Cli cp --only-show-errors ${debug}${acl}${storageEncryption}${storageKmsKeyId}--storage-class $storageClass "\$name" "\$s3path/\$name"
            fi
        }
        
        nxf_s3_download() {
            local source=\$1
            local target=\$2
            local file_name=\$(basename \$1)
            
            predefined_prefixes=("s3://results." "s3://seqruns-" "s3://reads." "s3://references.")
            found=false
            for prefix in "\${predefined_prefixes[@]}"; do
                if [[ \$source == \$prefix* ]]; then
                    found=true
                    break
                fi
            done
            
            if [ "\$found" = true ]; then
                local is_dir=\$($defaultS3Cli ls \$source | grep -F "PRE \${file_name}/" -c)
                if [[ \$is_dir == 1 ]]; then
                    $defaultS3Cli cp --only-show-errors --recursive "\$source" "\$target"
                else 
                    $defaultS3Cli cp --only-show-errors "\$source" "\$target"
                fi
            else
                local is_dir=\$($s3Cli ls \$source | grep -F "PRE \${file_name}/" -c)
                if [[ \$is_dir == 1 ]]; then
                    $s3Cli cp --only-show-errors --recursive "\$source" "\$target"
                else 
                    $s3Cli cp --only-show-errors "\$source" "\$target"
                fi    
            fi 
        }
        """.stripIndent(true)
    }


    String render() {
        super.render() + retryEnv() + s3Lib()
    }

    static private S3BashLib lib0(AwsOptions opts, boolean includeCore) {
        new S3BashLib()
                .includeCoreFun(includeCore)
                .withMaxParallelTransfers( opts.maxParallelTransfers )
                .withDelayBetweenAttempts(opts.delayBetweenAttempts )
                .withMaxTransferAttempts( opts.maxTransferAttempts )
                .withS3CliPath( opts.s3Cli )
                .withDefaultS3Cli( opts.defaultS3Cli )
                .withStorageClass(opts.storageClass )
                .withStorageEncryption( opts.storageEncryption )
                .withStorageKmsKeyId( opts.storageKmsKeyId )
                .withRetryMode( opts.retryMode )
                .withDebug( opts.debug )
                .withAcl( opts.s3Acl )
    }

    static String script(AwsOptions opts) {
        lib0(opts,true).render()
    }

    static String script() {
        final opts = new AwsOptions(Global.session as Session)
        lib0(opts,false).render()
    }
}
