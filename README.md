# mmtf-proteomics
Methods for mapping proteomics data onto 3D protein structure.

A few example Jupyter Notebooks are available for testing and feedback.

* Cysteine Oxidation
    * [CysOxidationTo3DStructure.ipynb](https://mybinder.org/v2/gh/sbl-sdsc/mmtf-proteomics/master?filepath=notebooks%2Fanalysis%2FCysOxidationTo3DStructure.ipynb) Map S-Sulphenylation and S-Sulfinylations in A549 and HeLa cell lines to 3D structure
    * [CysOxidationProteomicAndStructuralEvidence.ipynb](https://mybinder.org/v2/gh/sbl-sdsc/mmtf-proteomics/master?filepath=notebooks%2Fanalysis%2FCysOxidationProteomicAndStructuralEvidence.ipynb) Same study as above with examples of 3D structural evidence
    * [CysOxidationInPDB.ipynb](https://mybinder.org/v2/gh/sbl-sdsc/mmtf-proteomics/master?filepath=notebooks%2Fanalysis%2FCysOxidationInPDB.ipynb) Table and 3D visualization of Cysteine oxidative PTMs found in 3D protein structure of the PDB
    * [S_sulphenylationTo3DStructure.ipynb](https://mybinder.org/v2/gh/sbl-sdsc/mmtf-proteomics/master?filepath=notebooks%2Fanalysis%2FS_sulphenylationTo3DStructure.ipynb) Map S-sulphenylation data to 3D structure

* Post-translational Modifications from dbPTM
    * [QueryDbPTM.ipynb](https://mybinder.org/v2/gh/sbl-sdsc/mmtf-proteomics/master?filepath=notebooks%2Fanalysis%2FQueryDbPTM.ipynb) Query the dbPTM database by UniProt Id, UniProt Name, or PDB/Chain Id  and map PTMs to 3D structure
    * [BrowseDbPTM.ipynb](https://mybinder.org/v2/gh/sbl-sdsc/mmtf-proteomics/master?filepath=notebooks%2Fanalysis%2FBrowseDbPTM.ipynb) Browse dbPTM database by PTM type and map to 3D structure

* Post-translational Modifications from PTMsigDB
    * [PTMsigDbTo3DStructure.ipynb](https://mybinder.org/v2/gh/sbl-sdsc/mmtf-proteomics/master?filepath=notebooks%2Fanalysis%2FPTMsigDbTo3DStructure.ipynb) Map annotated phosphorylation signatures in PTMsigDB to the 3D structure
    
* Post-translational Modifications from PDB
    * [QueryPdbPTM.ipynb](https://mybinder.org/v2/gh/sbl-sdsc/mmtf-proteomics/master?filepath=notebooks%2Fanalysis%2FQueryPdbPTM.ipynb) Query PTMs in the PDB by PTM type and map to 3D structure

## Run notebooks on CyVerse/VICE
The [VICE (Visual Interactive Computing Environment)](https://cyverse-visual-interactive-computing-environment.readthedocs-hosted.com/en/latest/index.html) in the [CyVerse Discovery Environment](https://www.cyverse.org/discovery-environment) enables users to run Jupyter Lab in a production environment. To use VICE, sign up for a free [CyVerse account](https://www.cyverse.org/create-account). 

The VICE environment supports large-scale analyses. Users can upload and download files, and save and share results of their analyses in their user accounts (up to 100GB of data). 

[![Vice](docs/vice_badge.png)](https://de.cyverse.org/de/?type=apps&app-id=00d83c10-9b9a-11e9-8421-008cfa5ae621&system-id=de)

[Follow these step to run Jupyter Lab on VICE](docs/vice_instructions.md) 

# How can I get involved in this project?
* Share proteomics data sets for 3D structure mapping and analysis
* Collaborate with us on a reproducible proteomics analysis
* Submit [feature requests and issues](https://github.com/sbl-sdsc/mmtf-proteomics/issues)
* Clone and use the project for your own research 
* Fork the project and submit a pull request with new features or bug fixes
* Use the project and present a talk at a lab meeting or conference
* Share your experiences with us and on social media

# Local Installation

[Mac and Linux](/docs/MacLinuxInstallation.md)

[Windows](/docs/WindowsInstallation.md)

# How to Cite this Work

Bradley AR, Rose AS, Pavelka A, Valasatava Y, Duarte JM, Prlić A, Rose PW (2017) MMTF - an efficient file format for the transmission, visualization, and analysis of macromolecular structures. PLOS Computational Biology 13(6): e1005575. doi: [10.1371/journal.pcbi.1005575](https://doi.org/10.1371/journal.pcbi.1005575)

Valasatava Y, Bradley AR, Rose AS, Duarte JM, Prlić A, Rose PW (2017) Towards an efficient compression of 3D coordinates of macromolecular structures. PLOS ONE 12(3): e0174846. doi: [10.1371/journal.pone.01748464](https://doi.org/10.1371/journal.pone.0174846)

Rose AS, Bradley AR, Valasatava Y, Duarte JM, Prlić A, Rose PW (2018) NGL viewer: web-based molecular graphics for large complexes, Bioinformatics, bty419. doi: [10.1093/bioinformatics/bty419](https://doi.org/10.1093/bioinformatics/bty419)

Rose AS, Bradley AR, Valasatava Y, Duarte JM, Prlić A, Rose PW (2016) Web-based molecular graphics for large complexes. In Proceedings of the 21st International Conference on Web3D Technology (Web3D '16). ACM, New York, NY, USA, 185-186. doi: [10.1145/2945292.2945324](https://doi.org/10.1145/2945292.2945324)

# Funding
This project is supported by the National Cancer Institute of the National Institutes of Health under Award Number U01CA198942. The content is solely the responsibility of the authors and does not necessarily represent the official views of the National Institutes of Health.
