nextflow.enable.dsl=2

/* input variables */
params.env       = null
params.result    = null
params.eukaryote = null

params.align = [
    seed: 15,
    reseed: 1.25,
    minscore: 19,
]

process download_reference {
    cpus { 1 }
    memory { 8.GB }
    debug true

    storeDir "${params.directory.assets}/genome"

    input:
    val taxon

    output:
    path "${taxon}.fa.gz"

    script:
    """
    #!/usr/bin/env python3

    import re
    from ftplib import FTP

    def ftp_path(accession):
        match = re.search('(\\w+)_(\\d+).(\\d)', accession)
        if match:
            prefix = match.group(1)
            genome = match.group(2)
            number = match.group(3)
            groups = re.findall('.{3}', genome)
            return (
                prefix,
                '/'.join(groups),
                number,
            )
        else:
            raise ValueError(f"failed to parse accession: '{accession}'")

    ncbi = "ftp.ncbi.nlm.nih.gov"
    prefix, directory, version = ftp_path("${taxon}")
    with open("${taxon}.fa.gz", "wb") as io, FTP(ncbi) as ftp:
        ftp.login()
        ftp.cwd(f"genomes/all/{prefix}/{directory}")
        ok = False
        for f in ftp.mlsd():
            if "${taxon}" in f[0]:
                ftp.cwd(f[0])
                for g in ftp.mlsd():
                    if "cds" not in g[0] and "rna" not in g[0] and "_genomic.fna.gz" in g[0]:
                        ftp.retrbinary(f"RETR {g[0]}", io.write)
                        ok = True
                        break
            if ok:
                break

        ftp.cwd('/')
    """
}

process index_genome {
    cpus { 1 }
    memory { 64.GB }
    label 'cloud'
    debug true

    storeDir "${params.directory.assets}/index"

    input:
    path reference

    output:
    path "${reference.name}.{amb,ann,bwt.2bit.64,pac,0123}"

    shell:
    '''
    bwa-mem2 index -p !{reference.name} !{reference.name}
    '''
}

process read_alignment {
    cpus { 4 }
    memory { 32.GB }
    label 'cloud'
    debug true

    publishDir "${params.directory.results}/${params.env}/${result_id}/${params.eukaryote}"

    input:
    tuple val(result_id), path(reads)
    path index

    output:
    path "align.bam", emit: bam
    path "align.log", emit: log

    shell:
    '''
    set -o pipefail
    bwa-mem2 mem \
        -t !{task.cpus} \
        -k !{params.align.seed} \
        -r !{params.align.reseed} \
        -T !{params.align.minscore} \
        -o align.bam \
        !{index[0].name.split("[.]")[0..-2].join(".")} \
        !{reads.name} \
    2>&1 | tee align.log
    '''
}

workflow {
    if (params.env == null) {
        error "Error[input]: requires non-null compute environment"
    }

    if (params.result == null) {
        error "Error[input]: requires non-null result ids"
    }
    result = (params.result != null) ?
        Channel.fromList(
            (params.result as String).split(',') as List
        ) : 'nil'

    readsets = result.map {
        r -> tuple(
            r,
            "s3://results.${params.env}.kariusdx.com/${r}/import-readset/reads.fastq.gz"
        )
    }

    if (params.eukaryote == null) {
        error "Error[input]: requires non-null eukaryote to align against"
    }

    index = download_reference(params.eukaryote) | index_genome
    read_alignment(readsets, index)
}
